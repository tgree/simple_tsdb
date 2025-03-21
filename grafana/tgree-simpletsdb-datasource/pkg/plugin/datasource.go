package plugin

import (
	"context"
	"encoding/json"
	"encoding/binary"
	"fmt"
	"time"
	"net"
	"io"
	"unsafe"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	//"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	//"github.com/tgree/simple-tsdb/pkg/models"
)

const (
	CT_CREATE_DATABASE      uint32 = 0x60545A42
	CT_CREATE_MEASUREMENT   uint32 = 0xBB632CE1
	CT_WRITE_POINTS         uint32 = 0xEAF5E003
	CT_SELECT_POINTS_LIMIT  uint32 = 0x7446C560
	CT_SELECT_POINTS_LAST   uint32 = 0x76CF2220
	CT_DELETE_POINTS        uint32 = 0xD9082F2C
	CT_GET_SCHEMA           uint32 = 0x87E5A959

	DT_DATABASE             uint32 = 0x39385A4F   // <database>
	DT_MEASUREMENT          uint32 = 0xDC1F48F3   // <measurement>
	DT_SERIES               uint32 = 0x4E873749   // <series>
	DT_TYPED_FIELDS         uint32 = 0x02AC7330   // <f1>/<type1>,<f2>/<type2>,...
	DT_FIELD_LIST           uint32 = 0xBB62ACC3   // <f1>,<f2>,...
	DT_CHUNK                uint32 = 0xE4E8518F   // <chunk header>, then data
	DT_TIME_FIRST           uint32 = 0x55BA37B4   // <t0> (uint64_t)
	DT_TIME_LAST            uint32 = 0xC4EE45BA   // <t1> (uint64_t)
	DT_NLIMIT               uint32 = 0xEEF2BB02   // LIMIT <N> (uint64_t)
	DT_NLAST                uint32 = 0xD74F10A3   // LAST <N> (uint64_t)
	DT_END                  uint32 = 0x4E29ADCC   // end of command
	DT_STATUS_CODE          uint32 = 0x8C8C07D9   // <errno> (uint32_t)
	DT_FIELD_TYPE           uint32 = 0x7DB40C2A   // <type> (uint32_t)
	DT_FIELD_NAME           uint32 = 0x5C0D45C1   // <name>
	DT_READY_FOR_CHUNK      uint32 = 0x6000531C   // <max_data_len> (uint32_t)

	FT_BOOL uint8 = 1
	FT_U32 uint8  = 2
	FT_U64 uint8  = 3
	FT_F32 uint8  = 4
	FT_F64 uint8  = 5
	FT_I32 uint8  = 6
	FT_I64 uint8  = 7
)

var FT_MAP = map[uint8]string{
	FT_BOOL:	"bool",
	FT_U32:		"u32",
	FT_U64:		"u64",
	FT_F32:		"f32",
	FT_F64:		"f64",
	FT_I32:		"i32",
	FT_I64:		"i64",
}

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces - only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(ctx context.Context, _ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	return &Datasource{}, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct{}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	// Clean up datasource instance resources.
}

type datasourceModel struct {
	Database	string
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// create response struct
	response := backend.NewQueryDataResponse()

	// Unmarshal the backend database.
	var dm datasourceModel
	err := json.Unmarshal(req.PluginContext.DataSourceInstanceSettings.JSONData, &dm)
	if err != nil {
		return nil, err
	}

	// Open a connection to the TSDB server.
	tc, err := NewTSDBClient()
	if err != nil {
		return nil, err
	}
	defer tc.Close()

	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		res := d.query(ctx, req.PluginContext, tc, &dm, q)

		// save the response in a hashmap
		// based on with RefID as identifier
		response.Responses[q.RefID] = res
	}

	return response, nil
}

type queryModel struct {
	Measurement	string
	Series		string
	Field		string
}

func (d *Datasource) query(ctx context.Context, pCtx backend.PluginContext, tc *TSDBClient, dm *datasourceModel, query backend.DataQuery) backend.DataResponse {
	// Unmarshal the JSON into our queryModel.
	var qm queryModel
	err := json.Unmarshal(query.JSON, &qm)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}

	// Retrieve the schema for this measurement.
	schema, err := tc.GetSchema(dm.Database, qm.Measurement)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("get schema error: %v", err.Error()))
	}

	// Generate the SELECT operation.
	t0 := query.TimeRange.From.UnixNano()
	t1 := query.TimeRange.To.UnixNano()
	op, err := tc.NewSelectOp(schema, qm.Series, qm.Field, uint64(t0), uint64(t1), 0xFFFFFFFFFFFFFFFF)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("error creating select op: %v", err.Error()))
	}

	// Pull chunks of data from the server and append them to our running data.
	timestamps := []time.Time{}
	value := schema.MakeArray(qm.Field)
	for {
		rxc, err := op.ReadChunk()
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("error reading chunk: %v", err.Error()))
		}
		if rxc == nil {
			break
		}

		for _, time_ns := range rxc.timestamps {
			timestamps = append(timestamps, time.Unix(0, int64(time_ns)))
		}

		value = rxc.AppendToArray(value)
	}

	// Return the response.
	return backend.DataResponse{
		Frames: []*data.Frame{
			data.NewFrame(
				"response",
				data.NewField("time", nil, timestamps),
				data.NewField(qm.Field, nil, value),
			),
		},
	}
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(_ context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	/*
	res := &backend.CheckHealthResult{}
	config, err := models.LoadPluginSettings(*req.PluginContext.DataSourceInstanceSettings)

	if err != nil {
		res.Status = backend.HealthStatusError
		res.Message = "Unable to load settings"
		return res, nil
	}

	if config.Secrets.ApiKey == "" {
		res.Status = backend.HealthStatusError
		res.Message = "API key is missing"
		return res, nil
	}
	*/

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

type TSDBClient struct {
	conn	net.Conn
}

func NewTSDBClient() (*TSDBClient, error) {
	conn, err := net.Dial("tcp", "host.docker.internal:4000")
	if err != nil {
		return nil, err
	}
	return &TSDBClient{
		conn: conn,
	}, nil
}

func (self *TSDBClient) Close() {
	self.conn.Close()
}

func (self *TSDBClient) WriteU16(v uint16) error {
	return binary.Write(self.conn, binary.LittleEndian, v)
}

func (self *TSDBClient) WriteU32(v uint32) error {
	return binary.Write(self.conn, binary.LittleEndian, v)
}

func (self *TSDBClient) WriteU64(v uint64) error {
	return binary.Write(self.conn, binary.LittleEndian, v)
}

func (self *TSDBClient) WriteString(s string) error {
	_, err := self.conn.Write([]byte(s))
	return err
}

func (self *TSDBClient) WriteU64Token(token uint32, v uint64) error {
	err := self.WriteU32(token)
	if err != nil {
		return err
	}

	err = self.WriteU64(v)
	if err != nil {
		return err
	}

	return nil
}

func (self *TSDBClient) WriteStringToken(token uint32, s string) error {
	err := self.WriteU32(token)
	if err != nil {
		return err
	}

	err = self.WriteU16(uint16(len(s)))
	if err != nil {
		return err
	}

	err = self.WriteString(s)
	if err != nil {
		return err
	}

	return nil
}

func (self *TSDBClient) ReadU16() (uint16, error) {
	var v uint16

	err := binary.Read(self.conn, binary.LittleEndian, &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (self *TSDBClient) ReadU32() (uint32, error) {
	var v uint32

	err := binary.Read(self.conn, binary.LittleEndian, &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (self *TSDBClient) ReadI32() (int32, error) {
	var v int32

	err := binary.Read(self.conn, binary.LittleEndian, &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (self *TSDBClient) ReadString(size uint16) (string, error) {
	buf := make([]byte, size)
	n, err := io.ReadFull(self.conn, buf)
	if err != nil {
		return "", err
	}
	if n != int(size) {
		panic("Unexpected read length!")
	}
	return string(buf), nil
}

func (self *TSDBClient) GetSchema(database string, measurement string) (*Schema, error) {
	err := self.WriteU32(CT_GET_SCHEMA)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_DATABASE, database)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_MEASUREMENT, measurement)
	if err != nil {
		return nil, err
	}

	err = self.WriteU32(DT_END)
	if err != nil {
		return nil, err
	}

	schema := Schema{
		database:	database,
		measurement:	measurement,
		fields_map: 	map[string]*SchemaField{},
	}
	for {
		dt, err := self.ReadU32()
		if err != nil {
			return nil, err
		}
		if dt == DT_STATUS_CODE {
			sc, err := self.ReadI32()
			if err != nil {
				return nil, err
			}
			if sc != 0 {
				backend.Logger.Debug("Status", "status", sc)
				panic("Unexpected status")
			}

			return &schema, nil
		}
		if dt != DT_FIELD_TYPE {
			panic("Expected DT_FIELD_TYPE")
		}
		field_type, err := self.ReadU32()
		if err != nil {
			return nil, err
		}

		dt, err = self.ReadU32()
		if err != nil {
			return nil, err
		}
		if dt != DT_FIELD_NAME {
			panic("Expected DT_FIELD_NAME")
		}
		size, err := self.ReadU16()
		if err != nil {
			return nil, err
		}
		field_name, err := self.ReadString(size)
		if err != nil {
			return nil, err
		}

		schema.fields = append(schema.fields, field_name)
		schema.fields_map[field_name] = &SchemaField{
			name:		field_name,
			field_type:	uint8(field_type),
		}
	}
}

type SchemaField struct {
	name		string
	field_type	uint8
}

type Schema struct {
	database	string
	measurement	string
	fields		[]string
	fields_map	map[string]*SchemaField
}

func (self *Schema) String() string {
	return fmt.Sprintf("<%v, %v, %v, %v>", self.database, self.measurement, self.fields, self.fields_map)
}

func (self *Schema) MakeArray(field string) interface{} {
	switch self.fields_map[field].field_type {
	case FT_BOOL:
		return []uint8{}

	case FT_U32:
		return []uint32{}

	case FT_U64:
		return []uint64{}

	case FT_F32:
		return []float32{}

	case FT_F64:
		return []float64{}

	case FT_I32:
		return []int32{}

	case FT_I64:
		return []int64{}

	default:
		panic("Unknown field type!");
	}
}

func (sf *SchemaField) String() string {
	return fmt.Sprintf("<%v : %v>", sf.name, FT_MAP[sf.field_type])
}

type SelectOp struct {
	client		*TSDBClient
	schema		*Schema
	series		string
	field		string
	t0		uint64
	t1		uint64
	limit		uint64
	last_token	uint32
}

func (self *TSDBClient) NewSelectOp(schema *Schema, series string, field string, t0 uint64, t1 uint64, limit uint64) (*SelectOp, error) {
	op := SelectOp{
		client:		self,
		schema:		schema,
		series:		series,
		field:		field,
		t0:		t0,
		t1:		t1,
		limit:		limit,
	}

	err := self.WriteU32(CT_SELECT_POINTS_LIMIT)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_DATABASE, schema.database)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_MEASUREMENT, schema.measurement)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_SERIES, series)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_FIELD_LIST, field)
	if err != nil {
		return nil, err
	}

	err = self.WriteU64Token(DT_TIME_FIRST, t0)
	if err != nil {
		return nil, err
	}

	err = self.WriteU64Token(DT_TIME_LAST, t1)
	if err != nil {
		return nil, err
	}

	err = self.WriteU64Token(DT_NLIMIT, limit)
	if err != nil {
		return nil, err
	}

	err = self.WriteU32(DT_END)
	if err != nil {
		return nil, err
	}

	op.last_token, err = self.ReadU32()
	if err != nil {
		return nil, err
	}
	if op.last_token == DT_STATUS_CODE {
		sc, err := self.ReadI32()
		if err != nil {
			return nil, err
		}
		backend.Logger.Debug("Status", "status", sc)
		panic("Unexpected status")
	}

	return &op, nil
}

type RXChunk struct {
	op		*SelectOp
	npoints		uint32
	bitmap_offset	uint32
	data_offset     uint32
	data		[]byte
	timestamps	[]uint64
	bitmap          []uint64
}

func (self *SelectOp) ReadChunk() (*RXChunk, error) {
	if self.last_token == DT_END {
		dt, err := self.client.ReadU32()
		if err != nil {
			return nil, err
		}
		if dt != DT_STATUS_CODE {
			backend.Logger.Debug("Garbage token", "garbage_token", dt)
			panic("Expected DT_STATUS_CODE")
		}
		
		sc, err := self.client.ReadI32()
		if err != nil {
			return nil, err
		}
		if sc != 0 {
			backend.Logger.Debug("Status", "status", sc)
			panic("Unexpected status")
		}

		return nil, nil
	}

	if self.last_token != DT_CHUNK {
		panic("Expected DT_CHUNK or DT_END")
	}
	npoints, err := self.client.ReadU32()
	if err != nil {
		return nil, err
	}
	bitmap_offset, err := self.client.ReadU32()
	if err != nil {
		return nil, err
	}
	data_len, err := self.client.ReadU32()
	if err != nil {
		return nil, err
	}
	data := make([]byte, data_len)
	n, err := io.ReadFull(self.client.conn, data)
	if err != nil {
		return nil, err
	}
	if n != int(data_len) {
		backend.Logger.Debug("Bad read length", "expected len", data_len, "got len", n)
		panic("Unexpected read length!")
	}

	self.last_token, err = self.client.ReadU32()
	if err != nil {
		return nil, err
	}

	return NewChunk(self, npoints, bitmap_offset, data)
}

func NewChunk(op *SelectOp, npoints uint32, bitmap_offset uint32, data []byte) (*RXChunk, error) {
	p := unsafe.Pointer(&data[0])
	timestamps := unsafe.Slice((*uint64)(p), npoints)

	offset := npoints * 8
	bitmap_nslots := ((bitmap_offset + npoints + 63) / 64)
	p = unsafe.Pointer(&data[offset])
	bitmap := unsafe.Slice((*uint64)(p), bitmap_nslots)

	data_offset := offset + bitmap_nslots * 8
	return &RXChunk{
		op:		op,
		npoints:	npoints,
		bitmap_offset:	bitmap_offset,
		data_offset:    data_offset,
		data:		data,
		timestamps:     timestamps,
		bitmap:         bitmap,
	}, nil
}

func (self *RXChunk) AppendToArray(arr interface{}) interface{} {
	p := unsafe.Pointer(&self.data[self.data_offset])
	switch arr.(type) {
	case []float64:
		return append(arr.([]float64), unsafe.Slice((*float64)(p), self.npoints)...)

	case []float32:
		return append(arr.([]float32), unsafe.Slice((*float32)(p), self.npoints)...)

	case []uint64:
		return append(arr.([]uint64), unsafe.Slice((*uint64)(p), self.npoints)...)

	case []uint32:
		return append(arr.([]uint32), unsafe.Slice((*uint32)(p), self.npoints)...)

	case []int64:
		return append(arr.([]int64), unsafe.Slice((*int64)(p), self.npoints)...)

	case []int32:
		return append(arr.([]int32), unsafe.Slice((*int32)(p), self.npoints)...)

	case []uint8:
		return append(arr.([]uint8), unsafe.Slice((*uint8)(p), self.npoints)...)

	default:
		backend.Logger.Debug("Unknown field type!")
		panic("Unknown field type!")
	}
}

func (self *RXChunk) String() string {
	return fmt.Sprintf("<npoints %v, bitmap_offset %v>", self.npoints, self.bitmap_offset)
}
