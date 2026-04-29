package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPArrowCapabilityFromLoginMaxVersion(t *testing.T) {
	testCases := []struct {
		name string
		max  *int64
		want bool
	}{
		{name: "missing", max: nil, want: false},
		{name: "older", max: ptrInt64(1), want: false},
		{name: "exact", max: ptrInt64(2), want: true},
		{name: "newer", max: ptrInt64(3), want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := &APIClient{}
			client.setHTTPArrowCapability(tc.max)
			assert.Equal(t, tc.want, client.httpArrowCapability())
		})
	}
}

func TestDecodeArrowResponseMaterializesRows(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)

	resp := QueryResponse{
		ID: "query-1",
		Settings: &Settings{
			TimeZone: loc.String(),
		},
		Schema: &[]DataField{
			{Name: "id", Type: "Int32"},
			{Name: "name", Type: "String"},
			{Name: "d", Type: "Date"},
			{Name: "ts", Type: "Timestamp"},
		},
	}

	ts := time.Date(2025, 1, 16, 10, 1, 26, 739219000, loc)
	payload := buildArrowPayload(t, resp, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "d", Type: arrow.FixedWidthTypes.Date32},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: loc.String()}},
	}, func(builder *arrowarray.RecordBuilder) {
		builder.Field(0).(*arrowarray.Int32Builder).AppendValues([]int32{1}, nil)
		builder.Field(1).(*arrowarray.StringBuilder).AppendValues([]string{"alice"}, nil)
		builder.Field(2).(*arrowarray.Date32Builder).Append(arrow.Date32FromTime(ts))
		builder.Field(3).(*arrowarray.TimestampBuilder).AppendTime(ts)
	})

	decoded, err := decodeQueryResponse(&rawHTTPResponse{
		headers: http.Header{contentType: []string{arrowStreamContentType}},
		body:    payload,
	})
	require.NoError(t, err)
	require.Nil(t, decoded.Data)
	require.Len(t, decoded.typedRows, 1)
	require.Len(t, decoded.typedRows[0], 4)
	assert.Equal(t, "1", decoded.typedRows[0][0])
	assert.Equal(t, "alice", decoded.typedRows[0][1])

	dc := &DatabendConn{cfg: &Config{}}
	rows, err := dc.newNextRows(context.Background(), decoded)
	require.NoError(t, err)
	dest := make([]driver.Value, 4)
	require.NoError(t, rows.Next(dest))

	assert.Equal(t, "alice", dest[1])
	assert.Equal(t, time.Date(2025, 1, 16, 0, 0, 0, 0, time.UTC), dest[2])
	assert.Equal(t, ts, dest[3])
}

func TestDecodeArrowResponseDerivesSchemaFromArrowSchema(t *testing.T) {
	resp := QueryResponse{
		ID:       "query-1",
		Settings: &Settings{TimeZone: time.UTC.String()},
		// Simulate real Arrow responses where response_header schema may be absent or incomplete.
		Schema: &[]DataField{},
	}

	payload := buildArrowPayload(t, resp, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, func(builder *arrowarray.RecordBuilder) {
		builder.Field(0).(*arrowarray.Int32Builder).AppendValues([]int32{42}, nil)
		builder.Field(1).(*arrowarray.StringBuilder).AppendValues([]string{"derived"}, nil)
	})

	decoded, err := decodeQueryResponse(&rawHTTPResponse{
		headers: http.Header{contentType: []string{arrowStreamContentType}},
		body:    payload,
	})
	require.NoError(t, err)
	require.NotNil(t, decoded.Schema)
	require.Len(t, *decoded.Schema, 2)
	assert.Equal(t, "id", (*decoded.Schema)[0].Name)
	assert.Equal(t, "Int32", (*decoded.Schema)[0].Type)
	assert.Equal(t, "name", (*decoded.Schema)[1].Name)
	assert.Equal(t, "String", (*decoded.Schema)[1].Type)
	require.Nil(t, decoded.Data)
	require.Len(t, decoded.typedRows, 1)
	assert.Equal(t, "42", decoded.typedRows[0][0])
	assert.Equal(t, "derived", decoded.typedRows[0][1])
}

func TestDecodeArrowResponseMaterializesNullTypeAsNil(t *testing.T) {
	resp := QueryResponse{
		ID:       "query-null",
		Settings: &Settings{TimeZone: time.UTC.String()},
	}

	payload := buildArrowPayload(t, resp, []arrow.Field{
		{Name: "n", Type: arrow.Null, Nullable: true},
	}, func(builder *arrowarray.RecordBuilder) {
		builder.Field(0).(*arrowarray.NullBuilder).AppendNull()
	})

	decoded, err := decodeQueryResponse(&rawHTTPResponse{
		headers: http.Header{contentType: []string{arrowStreamContentType}},
		body:    payload,
	})
	require.NoError(t, err)
	require.Nil(t, decoded.Data)
	require.Len(t, decoded.typedRows, 1)
	assert.Nil(t, decoded.typedRows[0][0])
}

func TestDecodeArrowResponseMaterializesTimestampTZ(t *testing.T) {
	loc := time.FixedZone("", -8*3600)
	input := time.Date(2025, 1, 16, 2, 1, 26, 739219000, loc)

	resp := QueryResponse{
		ID:       "query-timestamp-tz",
		Settings: &Settings{TimeZone: time.UTC.String()},
		Schema: &[]DataField{
			{Name: "ts_tz", Type: "Timestamp_Tz"},
		},
	}

	field := arrow.Field{
		Name: "ts_tz",
		Type: &arrow.Decimal128Type{Precision: 38, Scale: 0},
		Metadata: arrow.NewMetadata(
			[]string{arrowExtensionKey},
			[]string{arrowExtensionTimestampWithTZ},
		),
	}

	payload := buildArrowPayload(t, resp, []arrow.Field{field}, func(builder *arrowarray.RecordBuilder) {
		value := decimal128.New(int64(-8*3600), uint64(input.UnixMicro()))
		builder.Field(0).(*arrowarray.Decimal128Builder).Append(value)
	})

	decoded, err := decodeQueryResponse(&rawHTTPResponse{
		headers: http.Header{contentType: []string{arrowStreamContentType}},
		body:    payload,
	})
	require.NoError(t, err)
	require.Nil(t, decoded.Data)
	require.Len(t, decoded.typedRows, 1)

	dc := &DatabendConn{cfg: &Config{}}
	rows, err := dc.newNextRows(context.Background(), decoded)
	require.NoError(t, err)
	dest := make([]driver.Value, 1)
	require.NoError(t, rows.Next(dest))

	got, ok := dest[0].(time.Time)
	require.True(t, ok)
	assert.Equal(t, input.UnixMicro(), got.UnixMicro())
	_, offset := got.Zone()
	assert.Equal(t, -8*3600, offset)
}

func TestDecodeArrowResponseMaterializesGeo(t *testing.T) {
	textValue := `{"type":"Point","coordinates":[60,37]}`
	binaryValue := []byte{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 78, 64, 0, 0, 0, 0, 0, 128, 66, 64}

	testCases := []struct {
		name     string
		field    arrow.Field
		settings *Settings
		input    []byte
		want     any
	}{
		{
			name: "geometry-wkb",
			field: arrow.Field{
				Name: "geom",
				Type: arrow.BinaryTypes.LargeBinary,
				Metadata: arrow.NewMetadata(
					[]string{arrowExtensionKey},
					[]string{arrowExtensionGeometry},
				),
			},
			settings: &Settings{GeometryOutputFormat: "WKB"},
			input:    binaryValue,
			want:     binaryValue,
		},
		{
			name: "geography-geojson",
			field: arrow.Field{
				Name: "geog",
				Type: arrow.BinaryTypes.LargeBinary,
				Metadata: arrow.NewMetadata(
					[]string{arrowExtensionKey},
					[]string{arrowExtensionGeography},
				),
			},
			settings: &Settings{GeometryOutputFormat: "GEOJSON"},
			input:    []byte(textValue),
			want:     textValue,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := QueryResponse{
				ID:       "query-geo",
				Settings: tc.settings,
				Schema:   &[]DataField{{Name: tc.field.Name, Type: dataTypeNameFromGeoExtension(tc.field)}},
			}

			payload := buildArrowPayload(t, resp, []arrow.Field{tc.field}, func(builder *arrowarray.RecordBuilder) {
				builder.Field(0).(*arrowarray.BinaryBuilder).Append(tc.input)
			})

			decoded, err := decodeQueryResponse(&rawHTTPResponse{
				headers: http.Header{contentType: []string{arrowStreamContentType}},
				body:    payload,
			})
			require.NoError(t, err)
			require.Len(t, decoded.typedRows, 1)
			require.Len(t, decoded.typedRows[0], 1)
			assert.Equal(t, tc.want, decoded.typedRows[0][0])
		})
	}
}

func TestQuerySyncUsesHTTPArrowAcrossPages(t *testing.T) {
	type requestSnapshot struct {
		SQL       string
		Accept    string
		HasArrow  bool
		RequestID string
	}

	var (
		mu         sync.Mutex
		requests   []requestSnapshot
		loginCount int
	)

	pageResp := QueryResponse{
		ID:      "query-user",
		NextURI: "/v1/query/final",
		Schema: &[]DataField{
			{Name: "id", Type: "Int32"},
			{Name: "name", Type: "String"},
		},
	}
	pagePayload := buildArrowPayload(t, pageResp, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, func(builder *arrowarray.RecordBuilder) {
		builder.Field(0).(*arrowarray.Int32Builder).AppendValues([]int32{7}, nil)
		builder.Field(1).(*arrowarray.StringBuilder).AppendValues([]string{"arrow"}, nil)
	})

	startResp := QueryResponse{
		ID:      "query-user",
		NextURI: "/v1/query/page/1",
		Schema:  pageResp.Schema,
	}
	startPayload := buildArrowPayload(t, startResp, []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, func(builder *arrowarray.RecordBuilder) {})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/login":
			mu.Lock()
			loginCount++
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			w.Header().Set(DatabendSessionIDHeader, "session-login")
			require.NoError(t, json.NewEncoder(w).Encode(LoginResponse{ServerMaxArrowResultVersion: ptrInt64(2)}))
		case "/v1/query":
			var req QueryRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

			mu.Lock()
			requests = append(requests, requestSnapshot{
				SQL:       req.SQL,
				Accept:    r.Header.Get(accept),
				HasArrow:  req.ArrowResultVersionMax != nil,
				RequestID: r.Header.Get(DatabendQueryIDHeader),
			})
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			w.Header().Set(contentType, arrowStreamContentType)
			_, err := w.Write(startPayload)
			require.NoError(t, err)
		case "/v1/query/page/1":
			mu.Lock()
			requests = append(requests, requestSnapshot{
				SQL:      "PAGE",
				Accept:   r.Header.Get(accept),
				HasArrow: false,
			})
			mu.Unlock()

			w.Header().Set(contentType, arrowStreamContentType)
			_, err := w.Write(pagePayload)
			require.NoError(t, err)
		case "/v1/query/final":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	cfg.QueryResultFormat = QueryResultFormatArrow
	client := NewAPIClientFromConfig(cfg)

	resp, err := client.QuerySync(context.Background(), "SELECT 7, 'arrow'")
	require.NoError(t, err)
	require.Nil(t, resp.Data)
	require.Len(t, resp.typedRows, 1)
	assert.Equal(t, "7", resp.typedRows[0][0])
	assert.Equal(t, "arrow", resp.typedRows[0][1])

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, loginCount)
	require.Len(t, requests, 2)
	assert.Equal(t, "SELECT 7, 'arrow'", requests[0].SQL)
	assert.Equal(t, arrowStreamContentType, requests[0].Accept)
	assert.True(t, requests[0].HasArrow)
	assert.Equal(t, "session-login.1", requests[0].RequestID)

	assert.Equal(t, "PAGE", requests[1].SQL)
	assert.Equal(t, arrowStreamContentType, requests[1].Accept)
}

func TestQuerySyncFallsBackToJSONWhenArrowRequested(t *testing.T) {
	var (
		mu         sync.Mutex
		accepts    []string
		hasArrow   []bool
		loginCount int
	)

	jsonResp := QueryResponse{
		ID:     "query-user",
		Schema: &[]DataField{{Name: "n", Type: "Int32"}},
		Data:   [][]*string{{strPtr("1")}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/login":
			mu.Lock()
			loginCount++
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(LoginResponse{ServerMaxArrowResultVersion: ptrInt64(2)}))
		case "/v1/query":
			var req QueryRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

			mu.Lock()
			accepts = append(accepts, r.Header.Get(accept))
			hasArrow = append(hasArrow, req.ArrowResultVersionMax != nil)
			mu.Unlock()

			w.Header().Set(contentType, jsonMediaType)
			require.NoError(t, json.NewEncoder(w).Encode(jsonResp))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	cfg.QueryResultFormat = QueryResultFormatArrow
	client := NewAPIClientFromConfig(cfg)

	resp, err := client.QuerySync(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, resp.Data, 1)
	require.NotNil(t, resp.Data[0][0])
	assert.Equal(t, "1", *resp.Data[0][0])

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, loginCount)
	require.Len(t, accepts, 1)
	assert.Equal(t, arrowStreamContentType, accepts[0])
	assert.True(t, hasArrow[0])
}

func TestQuerySyncUsesJSONForRestoredState(t *testing.T) {
	var (
		mu       sync.Mutex
		accepts  []string
		hasArrow []bool
	)

	jsonResp := QueryResponse{
		ID:     "query-user",
		Schema: &[]DataField{{Name: "n", Type: "Int32"}},
		Data:   [][]*string{{strPtr("1")}},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/query" {
			http.NotFound(w, r)
			return
		}
		var req QueryRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		mu.Lock()
		accepts = append(accepts, r.Header.Get(accept))
		hasArrow = append(hasArrow, req.ArrowResultVersionMax != nil)
		mu.Unlock()

		w.Header().Set(contentType, jsonMediaType)
		require.NoError(t, json.NewEncoder(w).Encode(jsonResp))
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	cfg.QueryResultFormat = QueryResultFormatArrow
	client := NewAPIClientFromConfig(cfg).WithState(&APIClientState{
		SessionID:    "session-1",
		QuerySeq:     3,
		SessionState: `{"database":"default","settings":{},"txn_state":"AutoCommit","need_sticky":false,"need_keep_alive":false}`,
	})

	resp, err := client.QuerySync(context.Background(), "SELECT 1")
	require.NoError(t, err)
	require.Len(t, resp.Data, 1)
	require.NotNil(t, resp.Data[0][0])
	assert.Equal(t, "1", *resp.Data[0][0])

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, accepts, 1)
	assert.Equal(t, jsonContentType, accepts[0])
	assert.False(t, hasArrow[0])
}

func TestPollQuerySkipsCapabilityProbeForRestoredState(t *testing.T) {
	type requestSnapshot struct {
		Method string
		Path   string
		Accept string
	}

	var (
		mu       sync.Mutex
		requests []requestSnapshot
	)

	pageResp := QueryResponse{
		ID:      "query-user",
		NextURI: "/v1/query/final",
		Schema: &[]DataField{
			{Name: "n", Type: "Int32"},
		},
	}
	pagePayload := buildArrowPayload(t, pageResp, []arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
	}, func(builder *arrowarray.RecordBuilder) {
		builder.Field(0).(*arrowarray.Int32Builder).AppendValues([]int32{7}, nil)
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, requestSnapshot{
			Method: r.Method,
			Path:   r.URL.Path,
			Accept: r.Header.Get(accept),
		})
		mu.Unlock()

		switch r.URL.Path {
		case "/v1/query/page/1":
			w.Header().Set(contentType, arrowStreamContentType)
			_, err := w.Write(pagePayload)
			require.NoError(t, err)
		case "/v1/query/final":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	cfg := testHTTPConfig(t, server.URL)
	cfg.QueryResultFormat = QueryResultFormatArrow
	client := NewAPIClientFromConfig(cfg).WithState(&APIClientState{
		SessionID:    "session-1",
		QuerySeq:     3,
		SessionState: `{"database":"default","settings":{},"txn_state":"AutoCommit","need_sticky":false,"need_keep_alive":false}`,
	})

	resp, err := client.PollQuery(context.Background(), "/v1/query/page/1")
	require.NoError(t, err)
	require.Len(t, resp.typedRows, 1)
	assert.Equal(t, "7", resp.typedRows[0][0])

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, requests, 1)
	assert.Equal(t, "GET", requests[0].Method)
	assert.Equal(t, "/v1/query/page/1", requests[0].Path)
	assert.Equal(t, arrowStreamContentType, requests[0].Accept)
}

func buildArrowPayload(t *testing.T, response QueryResponse, fields []arrow.Field, fill func(builder *arrowarray.RecordBuilder)) []byte {
	t.Helper()

	header, err := json.Marshal(response)
	require.NoError(t, err)

	meta := arrow.NewMetadata([]string{"response_header"}, []string{string(header)})
	schema := arrow.NewSchema(fields, &meta)

	builder := arrowarray.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	if fill != nil {
		fill(builder)
	}

	record := builder.NewRecord()
	defer record.Release()

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func dataTypeNameFromGeoExtension(field arrow.Field) string {
	value, ok := field.Metadata.GetValue(arrowExtensionKey)
	if !ok {
		return ""
	}

	switch value {
	case arrowExtensionGeometry:
		return "Geometry"
	case arrowExtensionGeography:
		return "Geography"
	default:
		return ""
	}
}

func testHTTPConfig(t *testing.T, rawURL string) *Config {
	t.Helper()

	u, err := url.Parse(rawURL)
	require.NoError(t, err)

	cfg := NewConfig()
	cfg.Host = u.Host
	cfg.User = "root"
	cfg.Password = "root"
	cfg.SSLMode = SSL_MODE_DISABLE
	return cfg
}
