package plugin

import (
	"errors"
	"context"
	"encoding/json"
	"encoding/binary"
	"fmt"
	"time"
	"net/http"
	"io"
	"unsafe"
	"strings"
	"crypto/tls"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/resource/httpadapter"
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
	CT_LIST_DATABASES       uint32 = 0x29200D6D
	CT_LIST_MEASUREMENTS    uint32 = 0x0FEB1399
	CT_LIST_SERIES          uint32 = 0x7B8238D6
	CT_ACTIVE_SERIES        uint32 = 0xF3B5093D
	CT_COUNT_POINTS         uint32 = 0x0E329B19
	CT_SUM_POINTS           uint32 = 0x90305A39
	CT_NOP                  uint32 = 0x22CF1296
	CT_AUTHENTICATE         uint32 = 0x0995EBDA

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
	DT_NPOINTS              uint32 = 0x5F469D08   // <npoints> (uint64_t)
	DT_WINDOW_NS            uint32 = 0x76F0C374   // <window_ns> (uint64_t)
	DT_SUMS_CHUNK           uint32 = 0x53FC76FC   // <chunk_npoints> (uint16_t)
	DT_USERNAME             uint32 = 0x6E39D1DE   // <username>
	DT_PASSWORD             uint32 = 0x602E5B01   // <password>

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
	_ backend.CallResourceHandler   = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(ctx context.Context, dsis backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var dm datasourceModel
	err := json.Unmarshal(dsis.JSONData, &dm)
	if err != nil {
		return nil, err
	}
	password, exists := dsis.DecryptedSecureJSONData["password"]
	if !exists {
		return nil, errors.New("Missing password")
	}
	d := &Datasource{
		hostname: dm.Hostname,
		username: dm.Username,
		password: password,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/databases", d.handleDatabases) // Possibly unused
	mux.HandleFunc("/measurements", d.handleMeasurements)
	mux.HandleFunc("/series", d.handleSeries)
	mux.HandleFunc("/fields", d.handleFields)
	d.resourceHandler = httpadapter.New(mux)
	return d, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	resourceHandler		backend.CallResourceHandler
	hostname		string
	username		string
	password		string
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	// Clean up datasource instance resources.
}

type datasourceModel struct {
	Database	string
	Hostname	string
	Username	string
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	instanceSettings := req.PluginContext.DataSourceInstanceSettings

	// create response struct
	response := backend.NewQueryDataResponse()

	// Unmarshal the backend database.
	var dm datasourceModel
	err := json.Unmarshal(instanceSettings.JSONData, &dm)
	if err != nil {
		return nil, err
	}

	// Open a connection to the TSDB server.
	password, exists := instanceSettings.DecryptedSecureJSONData["password"]
	if !exists {
		return nil, errors.New("Missing password")
	}
	tc, err := NewTSDBClient(dm.Hostname, dm.Username, password)
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

// These fields are defind directly by BasicQuery in src/types.ts.
// BasicQuery gets turned into JSON and then we unmarshal it into
// this struct here, discarding any JSON fields we don't care about.
type queryModel struct {
	// From BasicQuery.
	Measurement	string
	Series		string
	Field		string
	Transform       string
	Zoom            string
	Alias           string

	// From DataQuery.
	IntervalMs      uint64
}

func (d *Datasource) query(ctx context.Context, pCtx backend.PluginContext, tc *TSDBClient, dm *datasourceModel, query backend.DataQuery) backend.DataResponse {
	// Unmarshal the JSON into our queryModel.
	var qm queryModel
	err := json.Unmarshal(query.JSON, &qm)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("json unmarshal: %v", err.Error()))
	}
	backend.Logger.Debug("Query", "query", query)

	t0 := uint64(query.TimeRange.From.UnixNano())
	t1 := uint64(query.TimeRange.To.UnixNano())

	var seriesList []string
	if qm.Series == "All" {
		seriesList, err = tc.ListActiveSeries(dm.Database, qm.Measurement, t0, t1)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, "error from ListSeries")
		}
	} else {
		seriesList = strings.Split(qm.Series, " + ")
	}

	response := backend.DataResponse{Frames: []*data.Frame{}}

	for _, series := range seriesList {
		alias := series + "." + qm.Field
		if qm.Alias != "" {
			alias = strings.Replace(qm.Alias, "$series", series, 1)
		}
		// Retrieve the point count for this measurement.
		count_result, err := tc.CountPoints(dm.Database, qm.Measurement, series, t0, t1)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, "error from COUNT")
		}
		backend.Logger.Debug("Count Result", "count_result", count_result.String())
		if count_result.npoints == 0 {
			continue
		}

		var frame *data.Frame;
		if (count_result.npoints >= 10000) {
			switch qm.Zoom {
			case "Min/Max":
				frame, err = d.queryMinMax(tc, dm.Database, qm.Measurement, series, qm.Field, alias,
				                           t0, t1, qm.IntervalMs * 1000000)

			case "Mean":
				frame, err = d.queryMean(tc, dm.Database, qm.Measurement, series, qm.Field, alias,
				                         t0, t1, qm.IntervalMs * 1000000)
			}
		} else {
			frame, err = d.querySelect(tc, dm.Database, qm.Measurement, series, qm.Field, alias, t0, t1)
		}
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, "error from DB query")
		}
		if frame == nil {
			continue
		}

		switch qm.Transform {
		case "Tare":
			TransformTare(frame)
		case "Difference":
			TransformDifference(frame)
		case "Derivative (sec)":
			TransformDerivative(frame, 1)
		case "Derivative (min)":
			TransformDerivative(frame, 60)
		case "Derivative (hour)":
			TransformDerivative(frame, 3600)
		}

		response.Frames = append(response.Frames, frame)
	}

	return response
}

func (d *Datasource) querySelect(tc *TSDBClient, database string, measurement string, series string, field string, alias string, t0 uint64, t1 uint64) (*data.Frame, error) {
	// Retrieve the schema for this measurement.
	schema, err := tc.GetSchema(database, measurement)
	if err != nil {
		return nil, err
	}

	// Generate the SELECT operation.
	op, err := tc.NewSelectOp(schema, series, field, t0, t1, 0xFFFFFFFFFFFFFFFF)
	if err != nil {
		return nil, err
	}

	// Pull chunks of data from the server and append them to our running data.
	timestamps := []time.Time{}
	ptrs := schema.MakePtrArray(field)
	all_nil := true
	for {
		rxc, err := op.ReadChunk()
		if err != nil {
			return nil, err
		}
		if rxc == nil {
			break
		}

		for _, time_ns := range rxc.timestamps {
			timestamps = append(timestamps, time.Unix(0, int64(time_ns)))
		}

		var chunk_all_nil bool
		ptrs, chunk_all_nil = rxc.AppendToArray(ptrs)
		if !chunk_all_nil {
			all_nil = false
		}
	}
	
	// If no data, return empty frame.
	if all_nil {
		return nil, nil
	}

	// Return the response.
	return data.NewFrame(
		"response",
		data.NewField("time", nil, timestamps),
		data.NewField(alias, nil, ptrs),
	), nil
}

func (d *Datasource) queryMean(tc *TSDBClient, database string, measurement string, series string, field string, alias string, t0 uint64, t1 uint64, window_ns uint64) (*data.Frame, error) {
	// Generate the SUMS operation.
	op, err := tc.NewSumsOp(database, measurement, series, field, t0, t1, window_ns)
	if err != nil {
		return nil, err
	}

	// Pull chunks of data from the server and append them to our running data.
	// TODO: The number of buckets is known a priori...
	timestamps := []time.Time{}
	means := []float64{}
	ptrs := []*float64{}
	chunk_base := uint64(0)
	total_points := uint64(0)
	for {
		rxc, err := op.ReadChunk()
		if err != nil {
			return nil, err
		}
		if rxc == nil {
			break
		}

		for _, time_ns := range rxc.timestamps {
			timestamps = append(timestamps, time.Unix(0, int64(time_ns)))
		}
		for i := uint16(0); i < rxc.nbuckets; i++ {
			if rxc.npoints[i] == 0 {
				means = append(means, 0)
			} else {
				means = append(means, rxc.sums[i] / float64(rxc.npoints[i]))
			}
		}
		for i := uint16(0); i < rxc.nbuckets; i++ {
			total_points += rxc.npoints[i]
			if rxc.npoints[i] == 0 {
				ptrs = append(ptrs, nil)
			} else {
				ptrs = append(ptrs, &means[chunk_base + uint64(i)])
			}
		}

		chunk_base += uint64(rxc.nbuckets)
	}

	// If no data, return empty frame.
	if total_points == 0 {
		return nil, nil
	}

	// Return the response.
	return data.NewFrame(
		"response",
		data.NewField("time", nil, timestamps),
		data.NewField(alias, nil, ptrs),
	), nil
}

func (d *Datasource) queryMinMax(tc *TSDBClient, database string, measurement string, series string, field string, alias string, t0 uint64, t1 uint64, window_ns uint64) (*data.Frame, error) {
	// Retrieve the schema for this measurement.
	schema, err := tc.GetSchema(database, measurement)
	if err != nil {
		return nil, err
	}

	// Generate the SUMS operation.
	op, err := tc.NewSumsOp(database, measurement, series, field, t0, t1, window_ns)
	if err != nil {
		return nil, err
	}

	// Pull chunks of data from the server and append them to our running data.
	// TODO: The number of buckets is known a priori...
	timestamps := []time.Time{}
	ptrs := schema.MakePtrArray(field)
	have_non_nil := false
	for {
		rxc, err := op.ReadChunk()
		if err != nil {
			return nil, err
		}
		if rxc == nil {
			break
		}

		for i := uint16(0); i < rxc.nbuckets; i++ {
			// Grafana only has millisecond resolution!  Harsh.
			timestamps = append(timestamps, time.Unix(0, int64(rxc.timestamps[i])))
			timestamps = append(timestamps, time.Unix(0, int64(rxc.timestamps[i]) + 1000000))
			if rxc.npoints[i] == 0 {
				ptrs = rxc.AppendNil(ptrs)
				ptrs = rxc.AppendNil(ptrs)
			} else if !have_non_nil {
				ptrs = rxc.AppendMean(ptrs, i)
				ptrs = rxc.AppendMin(ptrs, i)
				have_non_nil = true
			} else {
				ptrs = rxc.AppendMax(ptrs, i)
				ptrs = rxc.AppendMin(ptrs, i)
			}
		}
	}

	if !have_non_nil {
		return nil, nil
	}

	// Return the response.
	return data.NewFrame(
		"response",
		data.NewField("time", nil, timestamps),
		data.NewField(alias, nil, ptrs),
	), nil
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

	// Open a connection to the TSDB server.
	tc, err := NewTSDBClient(d.hostname, d.username, d.password)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Unable to connect to TSDB server",
		}, nil
	}
	defer tc.Close()

	// Issue a NOP command.
	err = tc.NOP()
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "TSDB server didn't handle NOP command",
		}, nil
	}

	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Data source is working",
	}, nil
}

func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	return d.resourceHandler.CallResource(ctx, req, sender)
}

type databasesResponse struct {
	Databases	[]string	`json:"databases"`
}

func (d *Datasource) handleDatabases(rw http.ResponseWriter, req *http.Request) {
	// This *SEEMS* to be unused in the front end.  I think originally we were gonig to
	// make the database selectable in the QueryEditor, but then we forced each datasource
	// to be tied to a specific database, so now it has no use.
	tc, err := NewTSDBClient(d.hostname, d.username, d.password)
	if err != nil {
		return
	}
	defer tc.Close()

	rsp := databasesResponse{}
	rsp.Databases, err = tc.ListDatabases()
	if err != nil {
		panic("Error listing databases!")
		return
	}
	backend.Logger.Debug("Databases", "databases", rsp.Databases)

	bytes, err := json.Marshal(rsp)
	if err != nil {
		panic("Error marshalling response!")
		return
	}

	rw.Header().Add("Content-Type", "application/json")
	_, err = rw.Write(bytes)
	if err != nil {
		return
	}
	rw.WriteHeader(http.StatusOK)
}

type measurementsResponse struct {
	Measurements	[]string	`json:"measurements"`
}

func (d *Datasource) handleMeasurements(rw http.ResponseWriter, req *http.Request) {
	tc, err := NewTSDBClient(d.hostname, d.username, d.password)
	if err != nil {
		return
	}
	defer tc.Close()

	database := req.URL.Query().Get("database")
	if database == "" {
		return
	}

	rsp := measurementsResponse{}
	rsp.Measurements, err = tc.ListMeasurements(database)
	if err != nil {
		panic("Error listing measurements!")
		return
	}
	backend.Logger.Debug("Measurements", "measurements", rsp.Measurements)

	bytes, err := json.Marshal(rsp)
	if err != nil {
		panic("Error marshalling response!")
		return
	}

	rw.Header().Add("Content-Type", "application/json")
	_, err = rw.Write(bytes)
	if err != nil {
		return
	}
	rw.WriteHeader(http.StatusOK)
}

type seriesResponse struct {
	Series		[]string	`json:"series"`
}

func (d *Datasource) handleSeries(rw http.ResponseWriter, req *http.Request) {
	tc, err := NewTSDBClient(d.hostname, d.username, d.password)
	if err != nil {
		return
	}
	defer tc.Close()

	database := req.URL.Query().Get("database")
	if database == "" {
		return
	}

	measurement := req.URL.Query().Get("measurement")
	if measurement == "" {
		return
	}

	rsp := seriesResponse{}
	rsp.Series, err = tc.ListSeries(database, measurement)
	if err != nil {
		panic("Error listing series!")
		return
	}
	backend.Logger.Debug("Series", "series", rsp.Series)

	bytes, err := json.Marshal(rsp)
	if err != nil {
		panic("Error marshalling response!")
		return
	}

	rw.Header().Add("Content-Type", "application/json")
	_, err = rw.Write(bytes)
	if err != nil {
		return
	}
	rw.WriteHeader(http.StatusOK)
}

type fieldsResponse struct {
	Fields		[]string	`json:"fields"`
}

func (d *Datasource) handleFields(rw http.ResponseWriter, req *http.Request) {
	tc, err := NewTSDBClient(d.hostname, d.username, d.password)
	if err != nil {
		return
	}
	defer tc.Close()

	database := req.URL.Query().Get("database")
	if database == "" {
		return
	}

	measurement := req.URL.Query().Get("measurement")
	if measurement == "" {
		return
	}

	schema, err := tc.GetSchema(database, measurement)
	if err != nil {
		return
	}

	rsp := fieldsResponse{}
	rsp.Fields = schema.fields
	bytes, err := json.Marshal(rsp)
	if err != nil {
		panic("Error marshalling response!")
	}

	rw.Header().Add("Content-Type", "application/json")
	_, err = rw.Write(bytes)
	if err != nil {
		return
	}
	rw.WriteHeader(http.StatusOK)
}

type TSDBClient struct {
	conn	*tls.Conn
}

func NewTSDBClient(hostname string, username string, password string) (*TSDBClient, error) {
	conn, err := tls.Dial("tcp", hostname, &tls.Config{})
	if err != nil {
		return nil, err
	}
	client := &TSDBClient{
		conn: conn,
	}
	err = client.Authenticate(username, password)
	if err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
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

func (self *TSDBClient) ReadU64() (uint64, error) {
	var v uint64

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

func (self *TSDBClient) Authenticate(username string, password string) error {
	err := self.WriteU32(CT_AUTHENTICATE)
	if err != nil {
		return err
	}

	err = self.WriteStringToken(DT_USERNAME, username)
	if err != nil {
		return err
	}
	err = self.WriteStringToken(DT_PASSWORD, password)
	if err != nil {
		return err
	}
	err = self.WriteU32(DT_END)
	if err != nil {
		return err
	}

	dt, err := self.ReadU32()
	if err != nil {
		return err
	}
	if dt != DT_STATUS_CODE {
		panic("Expected DT_STATUS_CODE.")
	}
	sc, err := self.ReadI32()
	if err != nil {
		return err
	}
	if sc != 0 {
		backend.Logger.Debug("Status", "status", sc)
		panic("Unexpected AUTHENTICATE status")
	}

	return nil
}

func (self *TSDBClient) NOP() error {
	err := self.WriteU32(CT_NOP)
	if err != nil {
		return err
	}

	err = self.WriteU32(DT_END)
	if err != nil {
		return err
	}

	dt, err := self.ReadU32()
	if err != nil {
		return err
	}
	if dt != DT_STATUS_CODE {
		panic("Expected DT_STATUS_CODE.")
	}
	sc, err := self.ReadI32()
	if err != nil {
		return err
	}
	if sc != 0 {
		backend.Logger.Debug("Status", "status", sc)
		panic("Unexpected NOP status")
	}

	return nil
}

func (self *TSDBClient) ListDatabases() ([]string, error) {
	err := self.WriteU32(CT_LIST_DATABASES)
	if err != nil {
		return nil, err
	}

	err = self.WriteU32(DT_END)
	if err != nil {
		return nil, err
	}

	databases := []string{}
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

			return databases, nil
		}

		if dt != DT_DATABASE {
			panic("Expected DT_DATABASE")
		}
		size, err := self.ReadU16()
		if err != nil {
			return nil, err
		}
		name, err := self.ReadString(size)
		if err != nil {
			return nil, err
		}

		backend.Logger.Debug("Got Database", "database", name)
		databases = append(databases, name)
	}
}

func (self *TSDBClient) ListMeasurements(database string) ([]string, error) {
	err := self.WriteU32(CT_LIST_MEASUREMENTS)
	if err != nil {
		return nil, err
	}

	err = self.WriteStringToken(DT_DATABASE, database)
	if err != nil {
		return nil, err
	}

	err = self.WriteU32(DT_END)
	if err != nil {
		return nil, err
	}

	measurements := []string{}
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

			return measurements, nil
		}

		if dt != DT_MEASUREMENT {
			panic("Expected DT_MEASUREMENT")
		}
		size, err := self.ReadU16()
		if err != nil {
			return nil, err
		}
		name, err := self.ReadString(size)
		if err != nil {
			return nil, err
		}

		backend.Logger.Debug("Got Measurement", "measurement", name)
		measurements = append(measurements, name)
	}
}

func (self *TSDBClient) ListSeries(database string, measurement string) ([]string, error) {
	err := self.WriteU32(CT_LIST_SERIES)
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

	series := []string{}
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

			return series, nil
		}

		if dt != DT_SERIES {
			panic("Expected DT_SERIES")
		}
		size, err := self.ReadU16()
		if err != nil {
			return nil, err
		}
		name, err := self.ReadString(size)
		if err != nil {
			return nil, err
		}

		backend.Logger.Debug("Got Series", "series", name)
		series = append(series, name)
	}
}

func (self *TSDBClient) ListActiveSeries(database string, measurement string, t0 uint64, t1 uint64) ([]string, error) {
	err := self.WriteU32(CT_ACTIVE_SERIES)
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

	err = self.WriteU64Token(DT_TIME_FIRST, t0)
	if err != nil {
		return nil, err
	}

	err = self.WriteU64Token(DT_TIME_LAST, t1)
	if err != nil {
		return nil, err
	}

	err = self.WriteU32(DT_END)
	if err != nil {
		return nil, err
	}

	series := []string{}
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

			return series, nil
		}

		if dt != DT_SERIES {
			panic("Expected DT_SERIES")
		}
		size, err := self.ReadU16()
		if err != nil {
			return nil, err
		}
		name, err := self.ReadString(size)
		if err != nil {
			return nil, err
		}

		backend.Logger.Debug("Got Series", "series", name)
		series = append(series, name)
	}
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

type CountResult struct {
	time_first	uint64
	time_last	uint64
	npoints		uint64
}

func (self *CountResult) String() string {
	return fmt.Sprintf("<time_first: %v, time_last: %v, npoints: %v>", self.time_first, self.time_last, self.npoints)
}

func (self *TSDBClient) CountPoints(database string, measurement string, series string, t0 uint64, t1 uint64) (*CountResult, error) {
	err := self.WriteU32(CT_COUNT_POINTS)
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

	err = self.WriteStringToken(DT_SERIES, series)
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

	err = self.WriteU32(DT_END)
	if err != nil {
		return nil, err
	}

	dt, err := self.ReadU32()
	if err != nil {
		return nil, err
	}
	if dt == DT_STATUS_CODE {
		sc, err := self.ReadI32()
		if err != nil {
			return nil, err
		}
		backend.Logger.Debug("Status", "status", sc)
		panic("Unexpected status")
	}
	if dt != DT_TIME_FIRST {
		panic("Expected DT_TIME_FIRST")
	}
	time_first, err := self.ReadU64()
	if err != nil {
		return nil, err
	}

	dt, err = self.ReadU32()
	if err != nil {
		return nil, err
	}
	if dt != DT_TIME_LAST {
		panic("Expected DT_TIME_LAST")
	}
	time_last, err := self.ReadU64()
	if err != nil {
		return nil, err
	}

	dt, err = self.ReadU32()
	if err != nil {
		return nil, err
	}
	if dt != DT_NPOINTS {
		panic("Expected DT_NPOINTS")
	}
	npoints, err := self.ReadU64()
	if err != nil {
		return nil, err
	}

	dt, err = self.ReadU32()
	if err != nil {
		return nil, err
	}
	if dt != DT_STATUS_CODE {
		panic("Expected DT_STATUS_CODE")
	}
	sc, err := self.ReadI32()
	if err != nil {
		return nil, err
	}
	if sc != 0 {
		backend.Logger.Debug("Status", "status", sc)
		panic("Unexpected status")
	}

	return &CountResult{
		time_first:	time_first,
		time_last:	time_last,
		npoints:	npoints,
	}, nil
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

func (self *Schema) MakePtrArray(field string) interface{} {
	switch self.fields_map[field].field_type {
	case FT_BOOL:
		return []*uint8{}

	case FT_U32:
		return []*uint32{}

	case FT_U64:
		return []*uint64{}

	case FT_F32:
		return []*float32{}

	case FT_F64:
		return []*float64{}

	case FT_I32:
		return []*int32{}

	case FT_I64:
		return []*int64{}

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

func (self *RXChunk) IsNull(i uint32) bool {
	bitmap_index := (self.bitmap_offset + i) / 64
	shift := (self.bitmap_offset + i) % 64
	return (self.bitmap[bitmap_index] & (1 << shift)) == 0
}

func (self *RXChunk) AppendToArray(ptrs interface{}) (interface{}, bool) {
	p := unsafe.Pointer(&self.data[self.data_offset])
	all_nil := true
	switch ptrs.(type) {
	case []*float64:
		vf64 := unsafe.Slice((*float64)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*float64), nil)
			} else {
				ptrs = append(ptrs.([]*float64), &vf64[i])
				all_nil = false
			}
		}

	case []*float32:
		vf32 := unsafe.Slice((*float32)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*float32), nil)
			} else {
				ptrs = append(ptrs.([]*float32), &vf32[i])
				all_nil = false
			}
		}

	case []*uint64:
		vu64 := unsafe.Slice((*uint64)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*uint64), nil)
			} else {
				ptrs = append(ptrs.([]*uint64), &vu64[i])
				all_nil = false
			}
		}

	case []*uint32:
		vu32 := unsafe.Slice((*uint32)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*uint32), nil)
			} else {
				ptrs = append(ptrs.([]*uint32), &vu32[i])
				all_nil = false
			}
		}

	case []*int64:
		vi64 := unsafe.Slice((*int64)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*int64), nil)
			} else {
				ptrs = append(ptrs.([]*int64), &vi64[i])
				all_nil = false
			}
		}

	case []*int32:
		vi32 := unsafe.Slice((*int32)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*int32), nil)
			} else {
				ptrs = append(ptrs.([]*int32), &vi32[i])
				all_nil = false
			}
		}

	case []*uint8:
		vu8 := unsafe.Slice((*uint8)(p), self.npoints)
		for i := uint32(0); i < self.npoints; i++ {
			if self.IsNull(i) {
				ptrs = append(ptrs.([]*uint8), nil)
			} else {
				ptrs = append(ptrs.([]*uint8), &vu8[i])
				all_nil = false
			}
		}

	default:
		backend.Logger.Debug("Unknown field type!")
		panic("Unknown field type!")
	}

	return ptrs, all_nil
}

func (self *RXChunk) String() string {
	return fmt.Sprintf("<npoints %v, bitmap_offset %v>", self.npoints, self.bitmap_offset)
}

type SumsOp struct {
	client		*TSDBClient
	database	string
	measurement	string
	series		string
	field		string
	t0		uint64
	t1		uint64
	window_ns	uint64
	last_token	uint32
}

func (self *TSDBClient) NewSumsOp(database string, measurement string, series string, field string, t0 uint64, t1 uint64, window_ns uint64) (*SumsOp, error) {
	op := SumsOp{
		client:		self,
		database:	database,
		measurement:	measurement,
		series:		series,
		field:		field,
		t0:		t0,
		t1:		t1,
		window_ns:	window_ns,
	}

	err := self.WriteU32(CT_SUM_POINTS)
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

	err = self.WriteU64Token(DT_WINDOW_NS, window_ns)
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

type RXSumsChunk struct {
	op		*SumsOp
	nbuckets	uint16
	data		[]byte
	timestamps	[]uint64
	sums		[]float64
	mins		unsafe.Pointer
	maxs		unsafe.Pointer
	npoints		[]uint64
}

func (self *SumsOp) ReadChunk() (*RXSumsChunk, error) {
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

	if self.last_token != DT_SUMS_CHUNK {
		panic("Expected DT_SUMS_CHUNK or DT_END")
	}
	chunk_npoints, err := self.client.ReadU16()
	if err != nil {
		return nil, err
	}

	data_len := chunk_npoints * (8 + 1 * 32)	// 1 since only 1 field
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

	return NewSumsChunk(self, chunk_npoints, data)
}

func (self *RXSumsChunk) AppendNil(dst any) any {
	switch d := dst.(type) {
	case []*uint8:
		return append(d, nil)

	case []*uint32:
		return append(d, nil)

	case []*uint64:
		return append(d, nil)

	case []*float32:
		return append(d, nil)

	case []*float64:
		return append(d, nil)

	case []*int32:
		return append(d, nil)

	case []*int64:
		return append(d, nil)

	default:
		panic("Unhandled type!")
	}
}

func (self *RXSumsChunk) AppendMean(dst any, bucket uint16) any {
	npoints := self.npoints[bucket]
	var mean float64
	if npoints != 0 {
		mean = self.sums[bucket] / float64(self.npoints[bucket])
	}

	switch d := dst.(type) {
	case []*uint8:
		if npoints != 0 {
			v := uint8(mean)
			return append(d, &v)
		}
		return append(d, nil)

	case []*uint32:
		if npoints != 0 {
			v := uint32(mean)
			return append(d, &v)
		}
		return append(d, nil)

	case []*uint64:
		if npoints != 0 {
			v := uint64(mean)
			return append(d, &v)
		}
		return append(d, nil)

	case []*float32:
		if npoints != 0 {
			v := float32(mean)
			return append(d, &v)
		}
		return append(d, nil)

	case []*float64:
		if npoints != 0 {
			return append(d, &mean)
		}
		return append(d, nil)

	case []*int32:
		if npoints != 0 {
			v := int32(mean)
			return append(d, &v)
		}
		return append(d, nil)

	case []*int64:
		if npoints != 0 {
			v := int64(mean)
			return append(d, &v)
		}
		return append(d, nil)

	default:
		panic("Unhandled type!")
	}
}

func (self *RXSumsChunk) AppendMax(dst any, bucket uint16) any {
	switch d := dst.(type) {
	case []*uint8:
		v := unsafe.Slice((*uint8)(self.maxs), self.nbuckets * 8)
		return append(d, &v[bucket * 8])

	case []*uint32:
		v := unsafe.Slice((*uint32)(self.maxs), self.nbuckets * 2)
		return append(d, &v[bucket * 2])

	case []*uint64:
		v := unsafe.Slice((*uint64)(self.maxs), self.nbuckets)
		return append(d, &v[bucket])

	case []*float32:
		v := unsafe.Slice((*float32)(self.maxs), self.nbuckets * 2)
		return append(d, &v[bucket * 2])

	case []*float64:
		v := unsafe.Slice((*float64)(self.maxs), self.nbuckets)
		return append(d, &v[bucket])

	case []*int32:
		v := unsafe.Slice((*int32)(self.maxs), self.nbuckets * 2)
		return append(d, &v[bucket * 2])

	case []*int64:
		v := unsafe.Slice((*int64)(self.maxs), self.nbuckets)
		return append(d, &v[bucket])

	default:
		panic("Unhandled type!")
	}
}

func (self *RXSumsChunk) AppendMin(dst any, bucket uint16) any {
	switch d := dst.(type) {
	case []*uint8:
		v := unsafe.Slice((*uint8)(self.mins), self.nbuckets * 8)
		return append(d, &v[bucket * 8])

	case []*uint32:
		v := unsafe.Slice((*uint32)(self.mins), self.nbuckets * 2)
		return append(d, &v[bucket * 2])

	case []*uint64:
		v := unsafe.Slice((*uint64)(self.mins), self.nbuckets)
		return append(d, &v[bucket])

	case []*float32:
		v := unsafe.Slice((*float32)(self.mins), self.nbuckets * 2)
		return append(d, &v[bucket * 2])

	case []*float64:
		v := unsafe.Slice((*float64)(self.mins), self.nbuckets)
		return append(d, &v[bucket])

	case []*int32:
		v := unsafe.Slice((*int32)(self.mins), self.nbuckets * 2)
		return append(d, &v[bucket * 2])

	case []*int64:
		v := unsafe.Slice((*int64)(self.mins), self.nbuckets)
		return append(d, &v[bucket])

	default:
		panic("Unhandled type!")
	}
}

func NewSumsChunk(op *SumsOp, chunk_npoints uint16, data []byte) (*RXSumsChunk, error) {
	pos := uint32(0)
	dpos := 8 * uint32(chunk_npoints)

	p := unsafe.Pointer(&data[pos])
	timestamps := unsafe.Slice((*uint64)(p), chunk_npoints)
	pos += dpos

	p = unsafe.Pointer(&data[pos])
	sums := unsafe.Slice((*float64)(p), chunk_npoints)
	pos += dpos

	p = unsafe.Pointer(&data[pos])
	mins := p
	pos += dpos

	p = unsafe.Pointer(&data[pos])
	maxs := p
	pos += dpos

	p = unsafe.Pointer(&data[pos])
	npoints := unsafe.Slice((*uint64)(p), chunk_npoints)

	return &RXSumsChunk{
		op:		op,
		nbuckets:	chunk_npoints,
		data:		data,
		timestamps:	timestamps,
		sums:		sums,
		mins:           mins,
		maxs:           maxs,
		npoints:	npoints,
	}, nil
}

func TransformTare(frame *data.Frame) {
	nrows := frame.Rows()
	if nrows == 0 {
		return
	}

	for _, field := range frame.Fields {
		if field.Name == "time" {
			continue
		}

		t0 := any(nil)
		i := 0
		for ; i<nrows; i++ {
			if !field.NilAt(i) {
				var ok bool
				t0, ok = field.ConcreteAt(i)
				if ok {
					break;
				}
			}
		}
		for ; i<nrows; i++ {
			f, ok := field.ConcreteAt(i)
			if !ok {
				continue
			}

			switch v := t0.(type) {
			case float64:
				field.SetConcrete(i, f.(float64) - v)
			case float32:
				field.SetConcrete(i, f.(float32) - v)
			case uint64:
				field.SetConcrete(i, f.(uint64) - v)
			case uint32:
				field.SetConcrete(i, f.(uint32) - v)
			case int64:
				field.SetConcrete(i, f.(int64) - v)
			case int32:
				field.SetConcrete(i, f.(int32) - v)
			case uint8:
				field.SetConcrete(i, f.(uint8) - v)
			default:
				panic("Bad type!")
			}
		}
	}
}

func TransformDifference(frame *data.Frame) {
	nrows := frame.Rows()
	if nrows == 0 {
		return
	}

	for _, field := range frame.Fields {
		if field.Name == "time" {
			continue
		}

		for i := nrows - 1; i > 0; i-- {
			f1, ok := field.ConcreteAt(i)
			if !ok {
				continue
			}
			f0, ok := field.ConcreteAt(i - 1)
			if !ok {
				field.Set(i, nil)
				continue
			}

			switch v := f1.(type) {
			case float64:
				field.SetConcrete(i, v - f0.(float64))
			case float32:
				field.SetConcrete(i, v - f0.(float32))
			case uint64:
				field.SetConcrete(i, v - f0.(uint64))
			case uint32:
				field.SetConcrete(i, v - f0.(uint32))
			case int64:
				field.SetConcrete(i, v - f0.(int64))
			case int32:
				field.SetConcrete(i, v - f0.(int32))
			case uint8:
				field.SetConcrete(i, v - f0.(uint8))
			default:
				panic("Bad type!")
			}
		}
		field.Set(0, nil)
	}
}

func TransformDerivative(frame *data.Frame, periodSecs float64) {
	nrows := frame.Rows()
	if nrows == 0 {
		return
	}

	timeField, _ := frame.FieldByName("time")
	if timeField == nil {
		return
	}

	for j, field := range frame.Fields {
		if field.Name == "time" {
			continue
		}

		newField := data.NewFieldFromFieldType(data.FieldTypeNullableFloat64, nrows)
		newField.Name = field.Name

		for i := nrows - 1; i > 0; i-- {
			newField.Set(i, nil)

			f1, ok := field.ConcreteAt(i)
			if !ok {
				continue
			}
			f0, ok := field.ConcreteAt(i - 1)
			if !ok {
				continue
			}
			
			t1 := timeField.At(i).(time.Time)
			t0 := timeField.At(i-1).(time.Time)
			dt := float64(t1.UnixNano() - t0.UnixNano()) / float64(1000000000.)

			switch v := f1.(type) {
			case float64:
				newField.SetConcrete(i, periodSecs * (v - f0.(float64)) / dt)
			case float32:
				newField.SetConcrete(i, periodSecs * float64(v - f0.(float32)) / dt)
			case uint64:
				newField.SetConcrete(i, periodSecs * float64(v - f0.(uint64)) / dt)
			case uint32:
				newField.SetConcrete(i, periodSecs * float64(v - f0.(uint32)) / dt)
			case int64:
				newField.SetConcrete(i, periodSecs * float64(v - f0.(int64)) / dt)
			case int32:
				newField.SetConcrete(i, periodSecs * float64(v - f0.(int32)) / dt)
			case uint8:
				newField.SetConcrete(i, periodSecs * float64(v - f0.(uint8)) / dt)
			default:
				panic("Bad type!")
			}
		}
		newField.Set(0, nil)

		frame.Fields[j] = newField
	}
}
