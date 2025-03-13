# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
import socket
import struct
import math

import numpy as np


# Command tokens.
CT_CREATE_DATABASE      = 0x60545A42
CT_CREATE_MEASUREMENT   = 0xBB632CE1
CT_WRITE_POINTS         = 0xEAF5E003
CT_SELECT_POINTS_LIMIT  = 0x7446C560
CT_SELECT_POINTS_LAST   = 0x76CF2220
CT_DELETE_POINTS        = 0xD9082F2C
CT_GET_SCHEMA           = 0x87E5A959

# Data tokens
DT_DATABASE             = 0x39385A4F
DT_MEASUREMENT          = 0xDC1F48F3
DT_SERIES               = 0x4E873749
DT_TYPED_FIELDS         = 0x02AC7330
DT_FIELD_LIST           = 0xBB62ACC3
DT_CHUNK                = 0xE4E8518F
DT_TIME_FIRST           = 0x55BA37B4
DT_TIME_LAST            = 0xC4EE45BA
DT_NLIMIT               = 0xEEF2BB02
DT_NLAST                = 0xD74F10A3
DT_END                  = 0x4E29ADCC
DT_STATUS_CODE          = 0x8C8C07D9
DT_FIELD_TYPE           = 0x7DB40C2A
DT_FIELD_NAME           = 0x5C0D45C1
DT_READY_FOR_CHUNK      = 0x6000531C

# Status codes.
SC_INIT_IO_ERROR                = -1
SC_CREATE_DATABASE_IO_ERROR     = -2
SC_CREATE_MEASUREMENT_IO_ERROR  = -3
SC_INVALID_MEASUREMENT          = -4
SC_INVALID_SERIES               = -5
SC_CORRUPT_SCHEMA_FILE          = -6
SC_NO_SUCH_FIELD                = -7
SC_END_OF_SELECT                = -8
SC_INCORRECT_WRITE_CHUNK_LEN    = -9
SC_OUT_OF_ORDER_TIMESTAMPS      = -10
SC_TIMESTAMP_OVERWRITE_MISMATCH = -11
SC_FIELD_OVERWRITE_MISMATCH     = -12
SC_BITMAP_OVERWRITE_MISMATCH    = -13
SC_TAIL_FILE_TOO_BIG            = -14
SC_TAIL_FILE_INVALID_SIZE       = -15
SC_INVALID_TIME_LAST            = -16
SC_NO_SUCH_SERIES               = -17
SC_NO_SUCH_DATABASE             = -18


class StatusException(Exception):
    def __init__(self, status_code):
        self.status_code = status_code

    def __repr__(self):
        return 'StatusException(%d)' % self.status_code


class ConnectionClosedException(Exception):
    pass


class ProtocolException(Exception):
    pass


class FieldType:
    def __init__(self, name, identifier, size, struct_key, np_type):
        self.name       = name
        self.identifier = identifier
        self.size       = size
        self.struct_key = struct_key
        self.np_type    = np_type


# Field types
FIELD_TYPES = {
    1 : FieldType('bool', 0, 1, 'B', np.ubyte),
    2 : FieldType('u32',  1, 4, 'I', np.uint32),
    3 : FieldType('u64',  2, 8, 'Q', np.uint64),
    4 : FieldType('f32',  3, 4, 'f', np.float32),
    5 : FieldType('f64',  4, 8, 'd', np.float64),
}


def ceil_div(n, d):
    return (n + d - 1) // d


def round_up(v, K):
    '''
    Rounds up to the nearest multiple of K.
    '''
    return math.ceil(v / K) * K


class Field:
    def __init__(self, field_type, name):
        self.field_type = field_type
        self.name       = name

    def __repr__(self):
        return '<%s %s>' % (self.field_type.name, self.name)

    def pack(self, points):
        '''
        A list of N points is packed as follows:
        
            uint64_t    bitmap[ceil(N/4)]
            point_type  point[N]
            uint8_t     padding[]

        The padding aligns the total length to a multiple of 8 bytes.
        '''
        bitmap = [0xFFFFFFFFFFFFFFFF] * ceil_div(len(points), 64)
        values = [p[self.name] for p in points]
        for i, v in enumerate(values):
            if v is None:
                values[i] = 0
                bitmap[i // 64] ^= (1 << i % 64)

        bitmap = np.array(bitmap, dtype=np.uint64)
        values = np.array(values, dtype=self.field_type.np_type)
        nbytes = len(points) * self.field_type.size
        if nbytes % 8:
            pad = bytes(8 - (nbytes % 8))
        else:
            pad = b''
        return b'' + bitmap.data + values.data + pad


class Schema:
    def __init__(self, fields):
        self.fields = fields

    def __repr__(self):
        return repr(self.fields)

    def pack_points(self, points, index, n):
        timestamps = [points[i]['time_ns'] for i in range(index, index + n)]
        timestamps = np.array(timestamps, dtype=np.uint64)
        data = b''
        for f in self.fields:
            data += f.pack(points)

        return b'' + timestamps.data + data

    def data_len_for_npoints(self, N):
        M = len(self.fields)
        S = sum(round_up(N * f.field_type.size, 8) for f in self.fields)
        return 8 * N + math.ceil(N / 64) * 8 * M + S

    def max_points_for_data_len(self, data_len):
        '''
        Suppose we have M fields.  We would like to calculate the maximum
        number of points, N, that could be packed into a buffer length of
        data_len bytes.
        
        We have the following function:

            len(N) = 8*N +                      # Timestamps
                     ceil(N/64)*8*M +           # Bitmaps
                     round_up_8(N*f1.size) +    # Data 1
                     round_up_8(N*f2.size) +    # Data 2
                     ...
                     round_up_8(N*fM.size)      # Data M

        If we require N to be a multiple of 64, that simplifies to:

            len(N) = 8*N +
                     N/8*M +
                     N*f1.size +
                     N*f2.size +
                     ...
                     N*fM.size
                   = N*(8 + f1.size + f2.size + ... + fM.size) + (N/8)*M

        We want len(N) < data_len:

            N*(8 + sum(f[i].size) + M/8) < data_len
            N < data_len / (8 + sum(f[i].size) + M/8)

        Select the multiple of 64 less than that number.
        '''
        M = len(self.fields)
        S = sum(f.field_type.size for f in self.fields)
        N = int((data_len / (8 + S + M/8)) / 64) * 64
        return N


class TSDBClient:
    def __init__(self, host='127.0.0.1', port=4000):
        self.addr = (host, port)
        self.socket = socket.create_connection(self.addr)

    def _sendall(self, data):
        self.socket.sendall(data)

    def _recvall(self, size):
        data = b''
        while len(data) != size:
            new_data = self.socket.recv(size - len(data))
            if not new_data:
                raise ConnectionClosedException()
            data += new_data
        return data

    def _recv_u32(self):
        return struct.unpack('<I', self._recvall(4))[0]

    def _recv_i32(self):
        return struct.unpack('<i', self._recvall(4))[0]

    def _transact(self, cmd):
        self._sendall(cmd)

        dt = self._recv_u32()
        assert dt == DT_STATUS_CODE
        sc = self._recv_i32()
        if sc != 0:
            raise StatusException(sc)

    def create_database(self, database):
        database = database.encode()
        cmd = struct.pack('<IIH%usI' % len(database),
                          CT_CREATE_DATABASE,
                          DT_DATABASE,
                          len(database),
                          database,
                          DT_END)
        self._transact(cmd)

    def create_measurement(self, database, measurement, typed_fields):
        database = database.encode()
        measurement = measurement.encode()
        typed_fields = typed_fields.encode()
        cmd = struct.pack('<IIH%usIH%usIH%usI' % (len(database),
                                                  len(measurement),
                                                  len(typed_fields)),
                          CT_CREATE_MEASUREMENT,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_TYPED_FIELDS, len(typed_fields), typed_fields,
                          DT_END)
        self._transact(cmd)

    def get_schema(self, database, measurement):
        database = database.encode()
        measurement = measurement.encode()
        cmd = struct.pack('<IIH%usIH%usI' % (len(database),
                                             len(measurement)),
                          CT_GET_SCHEMA,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_END)
        self._sendall(cmd)
        fields = []
        while True:
            dt = self._recv_u32()
            if dt == DT_STATUS_CODE:
                sc = self._recv_i32()
                if sc != 0:
                    raise StatusException(sc)
                return Schema(fields)

            assert dt == DT_FIELD_TYPE
            data = self._recvall(10)
            typ, dt_field_name, size = struct.unpack('<IIH', data)
            assert dt_field_name == DT_FIELD_NAME

            name = self._recvall(size)
            fields.append(Field(FIELD_TYPES[typ], name.decode()))

    def _write_points_begin(self, database, measurement, series):
        '''
        Starts a write operation, which grabs a write lock on the series in the
        database.  Upon success, returns the maximum data length of a chunk.
        '''
        database = database.encode()
        measurement = measurement.encode()
        series = series.encode()
        cmd = struct.pack('<IIH%usIH%usIH%us' % (len(database),
                                                 len(measurement),
                                                 len(series)),
                          CT_WRITE_POINTS,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_SERIES, len(series), series)
        self._sendall(cmd)

        dt = self._recv_u32()
        if dt == DT_STATUS_CODE:
            raise StatusException(self._recv_i32())
        if dt != DT_READY_FOR_CHUNK:
            raise ProtocolException()
        return self._recv_u32()

    def _write_points_chunk(self, npoints, bitmap_offset, data):
        '''
        Writes a chunk to the series.  Upon success, returns the maximum data
        length for the next chunk (which will always be the same as the max
        data length of the first chunk, and so can be safely ignored).
        '''
        cmd = struct.pack('<IIII', DT_CHUNK, npoints, bitmap_offset, len(data))
        self._sendall(cmd)
        self._sendall(data)

        dt = self._recv_u32()
        if dt == DT_STATUS_CODE:
            raise StatusException(self._recv_i32())
        if dt != DT_READY_FOR_CHUNK:
            raise ProtocolException()
        return self._recv_u32()

    def _write_points_end(self):
        cmd = struct.pack('<I', DT_END)
        self._transact(cmd)

    def write_points(self, database, measurement, series, schema, points):
        assert points
        max_data_len = self._write_points_begin(database, measurement, series)

        index = 0
        rem_points = len(points)
        N = schema.max_points_for_data_len(max_data_len)
        while rem_points:
            n = min(rem_points, N)
            data = schema.pack_points(points, index, n)
            self._write_points_chunk(len(points), 0, data)
            index += n
            rem_points -= n

        self._write_points_end()
