# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
import socket
import struct
import math
import ssl

import numpy as np


# Command tokens.
CT_CREATE_DATABASE      = 0x60545A42
CT_CREATE_MEASUREMENT   = 0xBB632CE1
CT_WRITE_POINTS         = 0xEAF5E003
CT_SELECT_POINTS_LIMIT  = 0x7446C560
CT_SELECT_POINTS_LAST   = 0x76CF2220
CT_DELETE_POINTS        = 0xD9082F2C
CT_GET_SCHEMA           = 0x87E5A959
CT_LIST_DATABASES       = 0x29200D6D
CT_LIST_MEASUREMENTS    = 0x0FEB1399
CT_LIST_SERIES          = 0x7B8238D6
CT_ACTIVE_SERIES        = 0xF3B5093D
CT_COUNT_POINTS         = 0x0E329B19
CT_SUM_POINTS           = 0x90305A39
CT_NOP                  = 0x22CF1296
CT_AUTHENTICATE         = 0x0995EBDA

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
DT_NPOINTS              = 0x5F469D08
DT_WINDOW_NS            = 0x76F0C374
DT_SUMS_CHUNK           = 0x53FC76FC
DT_USERNAME             = 0x6E39D1DE
DT_PASSWORD             = 0x602E5B01


# Status codes.
class StatusCode:
    INIT_IO_ERROR                = -1
    CREATE_DATABASE_IO_ERROR     = -2
    CREATE_MEASUREMENT_IO_ERROR  = -3
    INVALID_MEASUREMENT          = -4
    INVALID_SERIES               = -5
    CORRUPT_SCHEMA_FILE          = -6
    NO_SUCH_FIELD                = -7
    END_OF_SELECT                = -8
    INCORRECT_WRITE_CHUNK_LEN    = -9
    OUT_OF_ORDER_TIMESTAMPS      = -10
    TIMESTAMP_OVERWRITE_MISMATCH = -11
    FIELD_OVERWRITE_MISMATCH     = -12
    BITMAP_OVERWRITE_MISMATCH    = -13
    TAIL_FILE_TOO_BIG            = -14
    TAIL_FILE_INVALID_SIZE       = -15
    INVALID_TIME_LAST            = -16
    NO_SUCH_SERIES               = -17
    NO_SUCH_DATABASE             = -18
    NO_SUCH_MEASUREMENT          = -19
    MEASUREMENT_EXISTS           = -20
    USER_EXISTS                  = -21
    NO_SUCH_USER                 = -22
    NOT_A_TSDB_ROOT              = -23
    DUPLICATE_FIELD              = -24
    TOO_MANY_FIELDS              = -25
    INVALID_CONFIG_FILE          = -26
    INVALID_CHUNK_SIZE           = -27


class StatusException(Exception):
    def __init__(self, status_code):
        super().__init__(self, 'Status exception %d' % status_code)
        self.status_code = status_code


class ConnectionClosedException(Exception):
    pass


class ProtocolException(Exception):
    pass


class FieldType:
    def __init__(self, name, identifier, size, struct_key, np_type, idb_type):
        self.name       = name
        self.identifier = identifier
        self.size       = size
        self.struct_key = struct_key
        self.np_type    = np_type
        self.idb_type   = idb_type


# Field types
FIELD_TYPES = {
    1 : FieldType('bool', 1, 1, 'B', np.ubyte,   bool),
    2 : FieldType('u32',  2, 4, 'I', np.uint32,  int),
    3 : FieldType('u64',  3, 8, 'Q', np.uint64,  int),
    4 : FieldType('f32',  4, 4, 'f', np.float32, float),
    5 : FieldType('f64',  5, 8, 'd', np.float64, float),
    6 : FieldType('i32',  6, 4, 'i', np.int32,   int),
    7 : FieldType('i64',  7, 8, 'q', np.int64,   int),
}


def ceil_div(n, d):
    return (n + d - 1) // d


def round_up(v, K):
    '''
    Rounds up to the nearest multiple of K.
    '''
    return ceil_div(v, K) * K


class Field:
    def __init__(self, field_type, name):
        self.field_type = field_type
        self.name       = name

    def __repr__(self):
        return '<%s %s>' % (self.field_type.name, self.name)

    def pack(self, points, index, n):
        '''
        A list of N points is packed as follows:

            uint64_t    bitmap[ceil(N/4)]
            point_type  point[N]
            uint8_t     padding[]

        The padding aligns the total length to a multiple of 8 bytes.
        '''
        bitmap = [0xFFFFFFFFFFFFFFFF] * ceil_div(n, 64)
        values = [points[i][self.name] for i in range(index, index + n)]
        for i, v in enumerate(values):
            if v is None:
                values[i] = 0
                bitmap[i // 64] ^= (1 << i % 64)

        bitmap = np.array(bitmap, dtype=np.uint64)
        values = np.array(values, dtype=self.field_type.np_type)
        nbytes = n * self.field_type.size
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

    def get_field_type(self, name):
        for f in self.fields:
            if f.name == name:
                return f.field_type
        raise KeyError

    def typed_fields_str(self):
        return ','.join(['%s/%s' % (f.name, f.field_type.name)
                         for f in self.fields])

    def pack_points(self, points, index, n):
        timestamps = [points[i]['time_ns'] for i in range(index, index + n)]
        timestamps = np.array(timestamps, dtype=np.uint64)
        data = b''
        for f in self.fields:
            data += f.pack(points, index, n)

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
                     (N/8)*M +
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


class FieldData:
    def __init__(self, bitmap_offset, bitmap_data, field_data, field_type):
        self.bitmap_offset = bitmap_offset
        self.field_type = field_type
        self.bitmap = np.frombuffer(bitmap_data, dtype=np.uint64)
        self.values = np.frombuffer(field_data, dtype=field_type.np_type)

    def __len__(self):
        return len(self.values)

    def __getitem__(self, i):
        if i < 0 or i >= len(self.values):
            raise IndexError

        if not self.get_bitmap_bit(i):
            return None
        return self.values[i]

    def get_bitmap_bit(self, i):
        bitmap_i = (self.bitmap_offset + i) // 64
        shift    = (self.bitmap_offset + i) % 64
        v        = int(self.bitmap[bitmap_i])
        return v & (1 << shift)

    def to_idb_type(self, i):
        v = self[i]
        if v is None:
            return None
        return self.field_type.idb_type(v)


class RXChunk:
    def __init__(self, schema, fields, npoints, bitmap_offset, data):
        self.schema        = schema
        self.npoints       = npoints
        self.bitmap_offset = bitmap_offset
        self.data          = data

        data_view       = memoryview(data)
        self.timestamps = np.frombuffer(data_view[0:npoints*8], dtype=np.uint64)
        offset          = npoints*8

        self.fields   = {}
        bitmap_nbytes = math.ceil((bitmap_offset + npoints) / 64) * 8
        for name in fields:
            bitmap_data = data_view[offset:offset + bitmap_nbytes]
            offset += bitmap_nbytes

            ft = schema.get_field_type(name)
            data_len = ft.size*npoints
            field_data = data_view[offset:offset + data_len]
            offset += data_len

            if data_len % 8:
                offset += (8 - (data_len % 8))

            self.fields[name] = FieldData(bitmap_offset, bitmap_data,
                                          field_data, ft)

    def __repr__(self):
        return 'RXChunk(%u points)' % self.npoints


class SelectOP:
    def __init__(self, client, ct_op, database, measurement, series, schema,
                 fields, t0, t1, N):
        self.client = client
        self.schema = schema
        self.fields = fields or [se.name for se in schema.fields]

        dt_n = DT_NLAST if ct_op == CT_SELECT_POINTS_LAST else DT_NLIMIT

        database = database.encode()
        measurement = measurement.encode()
        series = series.encode()
        field_list = ','.join(self.fields).encode()
        cmd = struct.pack('<IIH%usIH%usIH%usIH%usIQIQIQI' % (len(database),
                                                             len(measurement),
                                                             len(series),
                                                             len(field_list)),
                          ct_op,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_SERIES, len(series), series,
                          DT_FIELD_LIST, len(field_list), field_list,
                          DT_TIME_FIRST, t0,
                          DT_TIME_LAST, t1,
                          dt_n, N,
                          DT_END)
        self.client._sendall(cmd)

        dt = self.client._recv_u32()
        if dt == DT_STATUS_CODE:
            raise StatusException(self.client._recv_i32())

        self.last_token = dt

    def read_chunk(self):
        if self.last_token == DT_END:
            if self.client._recv_u32() != DT_STATUS_CODE:
                raise ProtocolException('Expected DT_STATUS_CODE')
            if self.client._recv_i32() != 0:
                raise ProtocolException('Expected status 0')
            return None

        if self.last_token != DT_CHUNK:
            raise ProtocolException('Expected DT_CHUNK')
        npoints       = self.client._recv_u32()
        bitmap_offset = self.client._recv_u32()
        data_len      = self.client._recv_u32()
        data          = self.client._recvall(data_len)

        self.last_token = self.client._recv_u32()

        return RXChunk(self.schema, self.fields, npoints, bitmap_offset, data)


class RXSumsChunk:
    def __init__(self, fields, timestamps, sums, npoints):
        self.fields     = fields
        self.timestamps = timestamps
        self.sums       = sums
        self.npoints    = npoints


class SumsOP:
    def __init__(self, client, database, measurement, series, fields, t0, t1,
                 window_ns):
        self.client    = client
        self.fields    = fields
        self.window_ns = window_ns
        self.sums      = []
        self.npoints   = []

        database = database.encode()
        measurement = measurement.encode()
        series = series.encode()
        field_list = ','.join(self.fields).encode()
        cmd = struct.pack('<IIH%usIH%usIH%usIH%usIQIQIQI' % (len(database),
                                                             len(measurement),
                                                             len(series),
                                                             len(field_list)),
                          CT_SUM_POINTS,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_SERIES, len(series), series,
                          DT_FIELD_LIST, len(field_list), field_list,
                          DT_TIME_FIRST, t0,
                          DT_TIME_LAST, t1,
                          DT_WINDOW_NS, window_ns,
                          DT_END)
        self.client._sendall(cmd)

        dt = self.client._recv_u32()
        if dt == DT_STATUS_CODE:
            raise StatusException(self.client._recv_i32())

        self.last_token = dt

    def read_chunk(self):
        if self.last_token == DT_END:
            if self.client._recv_u32() != DT_STATUS_CODE:
                raise ProtocolException('Expected DT_STATUS_CODE')
            if self.client._recv_i32() != 0:
                raise ProtocolException('Expected status 0')
            return None

        if self.last_token != DT_SUMS_CHUNK:
            raise ProtocolException('Expected DT_SUMS_CHUNK')
        chunk_npoints = self.client._recv_u16()
        data_len      = chunk_npoints * (8 + len(self.fields) * 32)
        data          = self.client._recvall(data_len)
        data_view     = memoryview(data)
        pos           = 0
        sums          = []
        npoints       = []
        timestamps    = np.frombuffer(data_view[pos:pos + 8 * chunk_npoints],
                                      dtype=np.uint64)
        pos          += 8 * chunk_npoints
        for _ in range(len(self.fields)):
            sums.append(np.frombuffer(data_view[pos:pos + 8 * chunk_npoints],
                                      dtype=np.float64))
            pos += 8 * chunk_npoints
        pos += 8 * chunk_npoints * len(self.fields)     # mins
        pos += 8 * chunk_npoints * len(self.fields)     # maxs
        for _ in range(len(self.fields)):
            npoints.append(np.frombuffer(data_view[pos:pos + 8 * chunk_npoints],
                                         dtype=np.uint64))
            pos += 8 * chunk_npoints

        self.last_token = self.client._recv_u32()

        return RXSumsChunk(self.fields, timestamps, sums, npoints)


class CountResult:
    def __init__(self, time_first, time_last, npoints):
        self.time_first = time_first
        self.time_last  = time_last
        self.npoints    = npoints

    def __repr__(self):
        return 'CountResult(%u, %u, %u)' % (self.time_first,
                                            self.time_last,
                                            self.npoints)


class Connection:
    DEFAULT_SSL_CTX = None

    def __init__(self, host='127.0.0.1', port=4000, credentials=None):
        self.addr = (host, port)
        self.raw_socket = socket.create_connection(self.addr)
        if credentials:
            assert len(credentials) == 2
            if Connection.DEFAULT_SSL_CTX is None:
                Connection.DEFAULT_SSL_CTX = ssl.create_default_context()
            self.socket = Connection.DEFAULT_SSL_CTX.wrap_socket(
                    self.raw_socket, server_hostname=host)
            self.authenticate(*credentials)
        else:
            self.socket = self.raw_socket

    def close(self):
        self.socket.close()

    def _sendall(self, data):
        self.socket.sendall(data)

    def _recvall(self, size):
        data = b''
        while len(data) != size:
            new_data = self.socket.recv(size - len(data))
            if not new_data:
                raise ConnectionClosedException('Connection closed.')
            data += new_data
        return data

    def _recv_u16(self):
        return struct.unpack('<H', self._recvall(2))[0]

    def _recv_u32(self):
        return struct.unpack('<I', self._recvall(4))[0]

    def _recv_u64(self):
        return struct.unpack('<Q', self._recvall(8))[0]

    def _recv_i32(self):
        return struct.unpack('<i', self._recvall(4))[0]

    def _transact(self, cmd):
        self._sendall(cmd)

        dt = self._recv_u32()
        if dt != DT_STATUS_CODE:
            raise ProtocolException('Expected DT_STATUS_CODE')
        sc = self._recv_i32()
        if sc != 0:
            raise StatusException(sc)

    def authenticate(self, username, password):
        username = username.encode()
        password = password.encode()
        cmd = struct.pack('<IIH%usIH%usI' % (len(username),
                                             len(password)),
                          CT_AUTHENTICATE,
                          DT_USERNAME, len(username), username,
                          DT_PASSWORD, len(password), password,
                          DT_END)
        self._transact(cmd)

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

    def list_databases(self):
        cmd = struct.pack('<II',
                          CT_LIST_DATABASES,
                          DT_END)
        self._sendall(cmd)
        databases = []
        while True:
            dt = self._recv_u32()
            if dt == DT_STATUS_CODE:
                sc = self._recv_i32()
                if sc != 0:
                    raise StatusException(sc)
                return databases

            if dt != DT_DATABASE:
                raise ProtocolException('Expected DT_DATABASE')
            size = self._recv_u16()
            name = self._recvall(size)
            databases.append(name.decode())

    def list_measurements(self, database):
        database = database.encode()
        cmd = struct.pack('<IIH%usI' % len(database),
                          CT_LIST_MEASUREMENTS,
                          DT_DATABASE, len(database), database,
                          DT_END)
        self._sendall(cmd)
        measurements = []
        while True:
            dt = self._recv_u32()
            if dt == DT_STATUS_CODE:
                sc = self._recv_i32()
                if sc != 0:
                    raise StatusException(sc)
                return measurements

            if dt != DT_MEASUREMENT:
                raise ProtocolException('Expected DT_MEASUREMENT')
            size = self._recv_u16()
            name = self._recvall(size)
            measurements.append(name.decode())

    def list_series(self, database, measurement):
        database = database.encode()
        measurement = measurement.encode()
        cmd = struct.pack('<IIH%usIH%usI' % (len(database),
                                             len(measurement)),
                          CT_LIST_SERIES,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_END)
        self._sendall(cmd)
        series = []
        while True:
            dt = self._recv_u32()
            if dt == DT_STATUS_CODE:
                sc = self._recv_i32()
                if sc != 0:
                    raise StatusException(sc)
                return series

            if dt != DT_SERIES:
                raise ProtocolException('Expected DT_SERIES')
            size = self._recv_u16()
            name = self._recvall(size)
            series.append(name.decode())

    def list_active_series(self, database, measurement, t0, t1):
        database = database.encode()
        measurement = measurement.encode()
        cmd = struct.pack('<IIH%usIH%usIQIQI' % (len(database),
                                                 len(measurement)),
                          CT_ACTIVE_SERIES,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_TIME_FIRST, t0,
                          DT_TIME_LAST, t1,
                          DT_END)
        self._sendall(cmd)
        series = []
        while True:
            dt = self._recv_u32()
            if dt == DT_STATUS_CODE:
                sc = self._recv_i32()
                if sc != 0:
                    raise StatusException(sc)
                return series

            if dt != DT_SERIES:
                raise ProtocolException('Expected DT_SERIES')
            size = self._recv_u16()
            name = self._recvall(size)
            series.append(name.decode())

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

            if dt != DT_FIELD_TYPE:
                raise ProtocolException('Expected DT_FIELD_TYPE')
            data = self._recvall(10)
            typ, dt_field_name, size = struct.unpack('<IIH', data)
            if dt_field_name != DT_FIELD_NAME:
                raise ProtocolException('Expected DT_FIELD_NAME')

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
            raise ProtocolException('Expected DT_READY_FOR_CHUNK')
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
            raise ProtocolException('Expected DT_READY_FOR_CHUNK')
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
            assert schema.data_len_for_npoints(n) == len(data)
            assert len(data) <= max_data_len
            self._write_points_chunk(n, 0, data)
            index += n
            rem_points -= n

        self._write_points_end()

    def delete_points(self, database, measurement, series, t):
        '''
        Deletes all points up to and including t.
        '''
        database = database.encode()
        measurement = measurement.encode()
        series = series.encode()
        cmd = struct.pack('<IIH%usIH%usIH%usIQI' % (len(database),
                                                    len(measurement),
                                                    len(series)),
                          CT_DELETE_POINTS,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_SERIES, len(series), series,
                          DT_TIME_LAST, t,
                          DT_END)
        self._sendall(cmd)

        dt = self._recv_u32()
        if dt != DT_STATUS_CODE:
            raise ProtocolException('Expected DT_STATUS_CODE')
        sc = self._recv_i32()
        if sc != 0:
            raise StatusException(sc)

    def select_points(self, database, measurement, series, schema, fields, t0,
                      t1, N):
        return SelectOP(self, CT_SELECT_POINTS_LIMIT, database, measurement,
                        series, schema, fields, t0, t1, N)

    def select_last_points(self, database, measurement, series, schema,
                           fields, t0, t1, N):
        return SelectOP(self, CT_SELECT_POINTS_LAST, database, measurement,
                        series, schema, fields, t0, t1, N)

    def count_points(self, database, measurement, series, t0, t1):
        database = database.encode()
        measurement = measurement.encode()
        series = series.encode()
        cmd = struct.pack('<IIH%usIH%usIH%usIQIQI' % (len(database),
                                                      len(measurement),
                                                      len(series)),
                          CT_COUNT_POINTS,
                          DT_DATABASE, len(database), database,
                          DT_MEASUREMENT, len(measurement), measurement,
                          DT_SERIES, len(series), series,
                          DT_TIME_FIRST, t0,
                          DT_TIME_LAST, t1,
                          DT_END)
        self._sendall(cmd)

        dt = self._recv_u32()
        if dt == DT_STATUS_CODE:
            raise StatusException(self._recv_i32())

        if dt != DT_TIME_FIRST:
            raise ProtocolException('Expected DT_TIME_FIRST')
        time_first = self._recv_u64()

        dt = self._recv_u32()
        if dt != DT_TIME_LAST:
            raise ProtocolException('Expected DT_TIME_LAST')
        time_last = self._recv_u64()

        dt = self._recv_u32()
        if dt != DT_NPOINTS:
            raise ProtocolException('Expected DT_NPOINTS')
        npoints = self._recv_u64()

        dt = self._recv_u32()
        if dt != DT_STATUS_CODE:
            raise ProtocolException('Expected DT_STATUS_CODE')
        if self._recv_i32() != 0:
            raise ProtocolException('Expected status 0')

        return CountResult(time_first, time_last, npoints)

    def sum_points(self, database, measurement, series, fields, t0, t1,
                   window_ns):
        return SumsOP(self, database, measurement, series, fields, t0, t1,
                      window_ns)


class Client:
    def __init__(self, host='127.0.0.1', port=4000, credentials=None):
        self.host        = host
        self.port        = port
        self.credentials = credentials
        self.conn        = None

    def connect(self):
        assert self.conn is None
        self.conn = Connection(host=self.host, port=self.port,
                               credentials=self.credentials)

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def create_database(self, database):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.create_database(database)
        except ProtocolException:
            self.close()
            raise

    def create_measurement(self, database, measurement, typed_fields):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.create_measurement(database, measurement,
                                                typed_fields)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def list_databases(self):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.list_databases()
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def list_measurements(self, database):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.list_measurements(database)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def list_series(self, database, measurement):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.list_series(database, measurement)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def list_active_series(self, database, measurement, t0, t1):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.list_active_series(database, measurement, t0, t1)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def get_schema(self, database, measurement):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.get_schema(database, measurement)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def write_points(self, database, measurement, series, schema, points):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.write_points(database, measurement, series,
                                          schema, points)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def delete_points(self, database, measurement, series, t):
        '''
        Deletes all points up to and including t.
        '''
        if self.conn is None:
            self.connect()

        try:
            return self.conn.delete_points(database, measurement, series, t)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def select_points(self, database, measurement, series, schema, fields=None,
                      t0=0, t1=0xFFFFFFFFFFFFFFFF, N=0xFFFFFFFFFFFFFFFF):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.select_points(database, measurement, series,
                                           schema, fields, t0, t1, N)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def select_last_points(self, database, measurement, series, schema,
                           fields=None, t0=0, t1=0xFFFFFFFFFFFFFFFF,
                           N=0xFFFFFFFFFFFFFFFF):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.select_last_points(database, measurement, series,
                                                schema, fields, t0, t1, N)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def count_points(self, database, measurement, series, t0=0,
                     t1=0xFFFFFFFFFFFFFFFF):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.count_points(database, measurement, series, t0, t1)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise

    def sum_points(self, database, measurement, series, fields, t0, t1,
                   window_ns):
        if self.conn is None:
            self.connect()

        try:
            return self.conn.sum_points(database, measurement, series, fields,
                                        t0, t1, window_ns)
        except StatusException:
            raise
        except:  # noqa: E722
            self.close()
            raise
