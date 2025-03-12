# Copyright (c) 2025 by Terry Greeniaus.
# All rights reserved.
import socket
import struct


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


class FieldType:
    def __init__(self, name, identifier, size, struct_key):
        self.name       = name
        self.identifier = identifier
        self.size       = size
        self.struct_key = struct_key


# Field types
FIELD_TYPES = {
    0 : FieldType('bool', 0, 1, 'B'),
    1 : FieldType('u32',  1, 4, 'I'),
    2 : FieldType('u64',  2, 8, 'Q'),
    3 : FieldType('f32',  3, 4, 'f'),
    4 : FieldType('f64',  4, 8, 'd'),
}


def ceil_div(n, d):
    return (n + d - 1) // d


class Field:
    def __init__(self, field_type, name):
        self.field_type = field_type
        self.name       = name

    def __repr__(self):
        return '<%s %s>' % (self.field_type.name, self.name)

    def pack(self, points):
        bitmap = [0xFFFFFFFFFFFFFFFF] * ceil_div(len(points), 64)
        data   = [p[self.name] for p in points]
        for i, v in enumerate(data):
            if v is None:
                data[i] = 0
                bitmap[i // 64] ^= (1 << i % 64)

        bitmap = struct.pack('<%uQ' % len(bitmap), *bitmap)
        data = struct.pack('<%u%c' % (len(points), self.field_type.struct_key),
                           *data)
        if len(data) % 8:
            data += bytes(8 - (len(data) % 8))
        return bitmap + data


class TSDBClient:
    def __init__(self, host='127.0.0.1', port=4000):
        self.addr = (host, port)
        self.socket = None

    def _sendall(self, data):
        if self.socket is None:
            self.socket = socket.create_connection(self.addr)

        self.socket.sendall(data)

    def _recvall(self, size):
        data = b''
        while len(data) != size:
            data += self.socket.recv(size - len(data))
        return data

    def _transact(self, cmd):
        try:
            self._sendall(cmd)
            data = self._recvall(8)

            dt, status = struct.unpack('<II', data)
            assert dt == DT_STATUS_CODE
            assert status == 0
        finally:
            if self.socket is not None:
                self.socket.close()
                self.socket = None

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
            data = self._recvall(4)
            dt = struct.unpack('<I', data)[0]
            if dt == DT_END:
                break
            assert dt == DT_FIELD_TYPE

            data = self._recvall(10)
            typ, dt_field_name, size = struct.unpack('<IIH', data)
            assert dt_field_name == DT_FIELD_NAME

            name = self._recvall(size)
            fields.append(Field(FIELD_TYPES[typ], name.decode()))

        return fields

    def _write_points_begin(self, database, measurement, series):
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

    def _write_points_chunk(self, npoints, bitmap_offset, data):
        cmd = struct.pack('<IIII', DT_CHUNK, npoints, bitmap_offset, len(data))
        self._sendall(cmd)
        self._sendall(data)

    def _write_points_end(self):
        cmd = struct.pack('<I', DT_END)
        self._transact(cmd)

    def write_points(self, database, measurement, series, schema, points):
        assert points
        self._write_points_begin(database, measurement, series)

        timestamps = [p['time_ns'] for p in points]
        data = struct.pack('<%uQ' % len(points), *timestamps)
        for f in schema:
            data += f.pack(points)
        self._write_points_chunk(len(points), 0, data)
        self._write_points_end()
