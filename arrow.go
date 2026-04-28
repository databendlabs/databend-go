package godatabend

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	arrowarray "github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/pkg/errors"
)

const httpArrowResultVersionMax int64 = 2

var httpArrowVersionPattern = regexp.MustCompile(`(?i)v?(\d+)\.(\d+)\.(\d+)`)

const (
	arrowExtensionKey             = "Extension"
	arrowExtensionEmptyArray      = "EmptyArray"
	arrowExtensionEmptyMap        = "EmptyMap"
	arrowExtensionVariant         = "Variant"
	arrowExtensionBitmap          = "Bitmap"
	arrowExtensionGeometry        = "Geometry"
	arrowExtensionGeography       = "Geography"
	arrowExtensionInterval        = "Interval"
	arrowExtensionVector          = "Vector"
	arrowExtensionTimestampWithTZ = "TimestampTz"
)

type marshaledArrowArray interface {
	arrow.Array
	GetOneForMarshal(int) interface{}
}

type rawHTTPResponse struct {
	headers http.Header
	body    []byte
}

type queryTransport string

const (
	queryTransportAuto  queryTransport = ""
	queryTransportJSON  queryTransport = queryTransport(QueryResultFormatJSON)
	queryTransportArrow queryTransport = queryTransport(QueryResultFormatArrow)
)

type contextWithoutQueryID struct {
	context.Context
}

func (c contextWithoutQueryID) Value(key interface{}) interface{} {
	if key == ContextKeyQueryID {
		return nil
	}
	return c.Context.Value(key)
}

func isArrowResponse(headers http.Header) bool {
	return strings.HasPrefix(headers.Get(contentType), arrowStreamContentType)
}

func minHTTPArrowVersion() []int {
	return []int{1, 2, 899}
}

func parseServerVersion(version string) []int {
	matches := httpArrowVersionPattern.FindStringSubmatch(version)
	if len(matches) != 4 {
		return nil
	}

	parts := make([]int, 0, 3)
	for _, match := range matches[1:] {
		part, err := strconv.Atoi(match)
		if err != nil {
			return nil
		}
		parts = append(parts, part)
	}
	return parts
}

func isHTTPArrowVersionSupported(version string) bool {
	versionParts := parseServerVersion(version)
	if len(versionParts) == 0 {
		return false
	}
	minParts := minHTTPArrowVersion()
	for i := range minParts {
		if versionParts[i] < minParts[i] {
			return false
		}
		if versionParts[i] > minParts[i] {
			return true
		}
	}
	return true
}

func (c *APIClient) usesHTTPArrowTransport() bool {
	if c.queryResultFormat != QueryResultFormatArrow {
		return false
	}
	return c.httpArrowCapability()
}

func (c *APIClient) fetchServerVersion(ctx context.Context) (string, error) {
	restore := c.snapshotClientState()
	defer restore()

	resp, err := c.querySyncWithTransport(ctx, "SELECT version()", queryTransportJSON)
	if err != nil {
		return "", err
	}
	if len(resp.Data) == 0 || len(resp.Data[0]) == 0 || resp.Data[0][0] == nil {
		return "", errors.New("server version response is empty")
	}
	return *resp.Data[0][0], nil
}

func (c *APIClient) doRequestRaw(
	ctx context.Context,
	method, path string,
	req interface{},
	needSticky bool,
	acceptType string,
) (*rawHTTPResponse, error) {
	var err error
	reqBody := []byte{}
	if req != nil {
		reqBody, err = json.Marshal(req)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal request body")
		}
	}

	url := c.makeURL(path)
	httpReq, err := http.NewRequest(method, url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create http request")
	}
	httpReq = httpReq.WithContext(ctx)

	headers, err := c.makeHeaders(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to make request headers")
	}
	if needSticky && len(c.nodeID) != 0 {
		headers.Set(DatabendQueryStickyNode, c.nodeID)
	}
	if method == "GET" && len(c.nodeID) != 0 {
		headers.Set(DatabendQueryIDNode, c.nodeID)
	}
	headers.Set(contentType, jsonContentType)
	if acceptType == "" {
		acceptType = jsonContentType
	}
	headers.Set(accept, acceptType)
	httpReq.Header = headers

	if len(c.host) > 0 {
		httpReq.Host = c.host
	}

	authRetryLimit := 2
	for i := 1; i <= authRetryLimit; i++ {
		select {
		case <-ctx.Done():
			return nil, errors.Wrap(ctx.Err(), "context done")
		default:
		}

		httpResp, err := c.cli.Do(httpReq)
		if err != nil {
			return nil, errors.Wrap(ErrDoRequest, err.Error())
		}

		httpRespBody, readErr := func() ([]byte, error) {
			defer func() {
				_ = httpResp.Body.Close()
			}()
			return io.ReadAll(httpResp.Body)
		}()
		if readErr != nil {
			return nil, errors.Wrap(ErrReadResponse, readErr.Error())
		}

		if httpResp.StatusCode == http.StatusUnauthorized {
			if c.authMethod() == AuthMethodAccessToken && i < authRetryLimit {
				_, _ = c.accessTokenLoader.LoadAccessToken(context.Background(), true)
				continue
			}
			return nil, NewAPIError("authorization failed", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode > 500 {
			return nil, NewAPIError("please retry again later", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode == 500 {
			return nil, NewAPIError("internal server error", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode >= 400 {
			return nil, NewAPIError("please check your arguments", httpResp.StatusCode, httpRespBody)
		} else if httpResp.StatusCode != http.StatusOK {
			return nil, NewAPIError("unexpected HTTP StatusCode", httpResp.StatusCode, httpRespBody)
		}

		return &rawHTTPResponse{
			headers: httpResp.Header.Clone(),
			body:    httpRespBody,
		}, nil
	}

	return nil, errors.Errorf("failed to do request after %d retries", authRetryLimit)
}

func decodeQueryResponse(rawResp *rawHTTPResponse) (*QueryResponse, error) {
	if rawResp == nil {
		return nil, errors.New("empty query response")
	}
	if !isArrowResponse(rawResp.headers) {
		var resp QueryResponse
		if err := json.Unmarshal(rawResp.body, &resp); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal response body")
		}
		if err := materializeJSONQueryRows(&resp); err != nil {
			return nil, err
		}
		return &resp, nil
	}

	reader, err := ipc.NewReader(bytes.NewReader(rawResp.body))
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode arrow stream")
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema == nil {
		return nil, errors.New("missing arrow schema in response")
	}

	responseHeader, ok := schema.Metadata().GetValue("response_header")
	if !ok {
		return nil, errors.New("missing response_header metadata in arrow payload")
	}

	var resp QueryResponse
	if err := json.Unmarshal([]byte(responseHeader), &resp); err != nil {
		return nil, errors.Wrap(err, "failed to decode response_header")
	}
	if resp.Schema == nil || len(*resp.Schema) != len(schema.Fields()) {
		fields, err := dataFieldsFromArrowSchema(schema)
		if err != nil {
			return nil, err
		}
		resp.Schema = &fields
	}

	typedRows, err := arrowReaderToRows(reader, resp.Schema, resp.Settings)
	if err != nil {
		return nil, err
	}
	resp.typedRows = typedRows
	return &resp, nil
}

func arrowReaderToRows(reader *ipc.Reader, fields *[]DataField, settings *Settings) ([][]driver.Value, error) {
	typedRows := make([][]driver.Value, 0)
	for reader.Next() {
		record := reader.Record()
		batchTypedRows, err := arrowRecordToRows(record, fields, settings)
		if err != nil {
			return nil, err
		}
		typedRows = append(typedRows, batchTypedRows...)
	}
	if err := reader.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read arrow batches")
	}
	return typedRows, nil
}

func arrowRecordToRows(record arrow.Record, fields *[]DataField, settings *Settings) ([][]driver.Value, error) {
	if fields == nil {
		derivedFields, err := dataFieldsFromArrowSchema(record.Schema())
		if err != nil {
			return nil, err
		}
		fields = &derivedFields
	}
	if len(*fields) != int(record.NumCols()) {
		derivedFields, err := dataFieldsFromArrowSchema(record.Schema())
		if err != nil {
			return nil, err
		}
		fields = &derivedFields
		if len(*fields) != int(record.NumCols()) {
			return nil, errors.New("arrow record columns and schema do not match")
		}
	}

	location := time.UTC
	if settings != nil && settings.TimeZone != "" {
		loc, err := time.LoadLocation(settings.TimeZone)
		if err != nil {
			return nil, err
		}
		location = loc
	}

	descs := make([]*TypeDesc, len(*fields))
	for i, field := range *fields {
		desc, err := ParseTypeDesc(field.Type)
		if err != nil {
			return nil, err
		}
		descs[i] = desc.Normalize()
	}

	typedRows := make([][]driver.Value, 0, int(record.NumRows()))
	columns := record.Columns()
	for rowIdx := 0; rowIdx < int(record.NumRows()); rowIdx++ {
		typedRow := make([]driver.Value, len(columns))
		for colIdx, column := range columns {
			if descs[colIdx].Name == "Null" {
				typedRow[colIdx] = nil
				continue
			}
			if column.IsNull(rowIdx) {
				typedRow[colIdx] = nil
				continue
			}

			typedValue, err := materializeArrowDriverValue(descs[colIdx], column, rowIdx, location)
			if err != nil {
				return nil, err
			}
			typedRow[colIdx] = typedValue
		}
		typedRows = append(typedRows, typedRow)
	}
	return typedRows, nil
}

func formatArrowColumnValue(desc *TypeDesc, column arrow.Array, rowIdx int, location *time.Location) (string, error) {
	if desc != nil && desc.Name == "Timestamp_Tz" {
		if array, ok := column.(*arrowarray.Decimal128); ok {
			return formatArrowTimestampTZDecimalValue(array.Value(rowIdx))
		}
	}

	marshaled, ok := column.(marshaledArrowArray)
	if !ok {
		return "", fmt.Errorf("arrow column does not support row materialization: %T", column)
	}

	return formatArrowValue(desc, marshaled.GetOneForMarshal(rowIdx), location)
}

func materializeArrowDriverValue(desc *TypeDesc, column arrow.Array, rowIdx int, location *time.Location) (driver.Value, error) {
	if desc == nil {
		return formatArrowColumnValue(desc, column, rowIdx, location)
	}

	switch desc.Name {
	case "Null":
		return nil, nil
	case "Date":
		return materializeArrowDateDriverValue(column, rowIdx)
	case "Timestamp":
		return materializeArrowTimestampDriverValue(column, rowIdx, location)
	case "Timestamp_Tz":
		return materializeArrowTimestampTZDriverValue(column, rowIdx, location)
	default:
		return formatArrowColumnValue(desc, column, rowIdx, location)
	}
}

func materializeArrowDateDriverValue(column arrow.Array, rowIdx int) (driver.Value, error) {
	marshaled, ok := column.(marshaledArrowArray)
	if !ok {
		return nil, fmt.Errorf("arrow column does not support row materialization: %T", column)
	}

	switch value := marshaled.GetOneForMarshal(rowIdx).(type) {
	case time.Time:
		return value.UTC().Truncate(24 * time.Hour), nil
	case string:
		return time.Parse("2006-01-02", value)
	default:
		text, err := formatArrowDateValue(value)
		if err != nil {
			return nil, err
		}
		return time.Parse("2006-01-02", text)
	}
}

func materializeArrowTimestampDriverValue(column arrow.Array, rowIdx int, location *time.Location) (driver.Value, error) {
	marshaled, ok := column.(marshaledArrowArray)
	if !ok {
		return nil, fmt.Errorf("arrow column does not support row materialization: %T", column)
	}

	ts, err := parseArrowTimestampValue(marshaled.GetOneForMarshal(rowIdx), location)
	if err != nil {
		return nil, err
	}
	if location != nil {
		ts = ts.In(location)
	}
	return ts, nil
}

func materializeArrowTimestampTZDriverValue(column arrow.Array, rowIdx int, location *time.Location) (driver.Value, error) {
	if array, ok := column.(*arrowarray.Decimal128); ok {
		return arrowTimestampTZDecimalToTime(array.Value(rowIdx)), nil
	}

	marshaled, ok := column.(marshaledArrowArray)
	if !ok {
		return nil, fmt.Errorf("arrow column does not support row materialization: %T", column)
	}

	return parseArrowTimestampValue(marshaled.GetOneForMarshal(rowIdx), location)
}

func dataFieldsFromArrowSchema(schema *arrow.Schema) ([]DataField, error) {
	if schema == nil {
		return nil, errors.New("missing arrow schema in response")
	}

	fields := make([]DataField, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		dbType, err := dataTypeStringFromArrowField(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, DataField{
			Name: field.Name,
			Type: dbType,
		})
	}
	return fields, nil
}

func dataTypeStringFromArrowField(field arrow.Field) (string, error) {
	dbType, err := baseDataTypeStringFromArrowField(field)
	if err != nil {
		return "", err
	}
	if field.Nullable && dbType != "Null" {
		return fmt.Sprintf("Nullable(%s)", dbType), nil
	}
	return dbType, nil
}

func baseDataTypeStringFromArrowField(field arrow.Field) (string, error) {
	if extensionType, ok := field.Metadata.GetValue(arrowExtensionKey); ok {
		switch extensionType {
		case arrowExtensionEmptyArray:
			return "EmptyArray", nil
		case arrowExtensionEmptyMap:
			return "EmptyMap", nil
		case arrowExtensionVariant:
			return "Variant", nil
		case arrowExtensionBitmap:
			return "Bitmap", nil
		case arrowExtensionGeometry:
			return "Geometry", nil
		case arrowExtensionGeography:
			return "Geography", nil
		case arrowExtensionInterval:
			return "Interval", nil
		case arrowExtensionTimestampWithTZ:
			return "Timestamp_Tz", nil
		case arrowExtensionVector:
			fixedSizeList, ok := field.Type.(*arrow.FixedSizeListType)
			if !ok {
				return "", fmt.Errorf("unsupported vector arrow field %q with type %T", field.Name, field.Type)
			}
			if fixedSizeList.Elem().ID() != arrow.FLOAT32 {
				return "", fmt.Errorf("unsupported vector element type for field %q: %s", field.Name, fixedSizeList.Elem())
			}
			return fmt.Sprintf("Vector(%d)", fixedSizeList.Len()), nil
		default:
			return "", fmt.Errorf("unsupported arrow extension type %q", extensionType)
		}
	}

	switch dt := field.Type.(type) {
	case *arrow.NullType:
		return "Null", nil
	case *arrow.BooleanType:
		return "Boolean", nil
	case *arrow.Int8Type:
		return "Int8", nil
	case *arrow.Int16Type:
		return "Int16", nil
	case *arrow.Int32Type:
		return "Int32", nil
	case *arrow.Int64Type:
		return "Int64", nil
	case *arrow.Uint8Type:
		return "UInt8", nil
	case *arrow.Uint16Type:
		return "UInt16", nil
	case *arrow.Uint32Type:
		return "UInt32", nil
	case *arrow.Uint64Type:
		return "UInt64", nil
	case *arrow.Float32Type:
		return "Float32", nil
	case *arrow.Float64Type:
		return "Float64", nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.FixedSizeBinaryType:
		return "Binary", nil
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return "String", nil
	case *arrow.TimestampType:
		return "Timestamp", nil
	case *arrow.Date32Type:
		return "Date", nil
	case *arrow.Decimal64Type:
		return fmt.Sprintf("Decimal(%d, %d)", dt.Precision, dt.Scale), nil
	case *arrow.Decimal128Type:
		return fmt.Sprintf("Decimal(%d, %d)", dt.Precision, dt.Scale), nil
	case *arrow.Decimal256Type:
		return fmt.Sprintf("Decimal(%d, %d)", dt.Precision, dt.Scale), nil
	case *arrow.ListType:
		inner, err := dataTypeStringFromArrowField(dt.ElemField())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Array(%s)", inner), nil
	case *arrow.LargeListType:
		inner, err := dataTypeStringFromArrowField(dt.ElemField())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Array(%s)", inner), nil
	case *arrow.MapType:
		key, err := dataTypeStringFromArrowField(dt.KeyField())
		if err != nil {
			return "", err
		}
		value, err := dataTypeStringFromArrowField(dt.ItemField())
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Map(%s, %s)", key, value), nil
	case *arrow.StructType:
		parts := make([]string, 0, len(dt.Fields()))
		for _, innerField := range dt.Fields() {
			inner, err := dataTypeStringFromArrowField(innerField)
			if err != nil {
				return "", err
			}
			parts = append(parts, inner)
		}
		return fmt.Sprintf("Tuple(%s)", strings.Join(parts, ", ")), nil
	default:
		return "", fmt.Errorf("unsupported arrow data type for field %q: %T", field.Name, field.Type)
	}
}

func formatArrowValue(desc *TypeDesc, value interface{}, location *time.Location) (string, error) {
	if desc == nil {
		return formatArrowScalar(value)
	}

	switch desc.Name {
	case "Date":
		return formatArrowDateValue(value)
	case "Timestamp":
		return formatArrowTimestampValue(value, location)
	case "Timestamp_Tz":
		return formatArrowTimestampTZValue(value, location)
	case "Binary":
		if raw, ok := value.([]byte); ok {
			return strings.ToUpper(hex.EncodeToString(raw)), nil
		}
	}
	return formatArrowScalar(value)
}

func formatArrowDateValue(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		if _, err := time.Parse("2006-01-02", v); err != nil {
			return "", err
		}
		return v, nil
	case time.Time:
		return v.UTC().Format("2006-01-02"), nil
	default:
		return formatArrowScalar(v)
	}
}

func formatArrowTimestampValue(value interface{}, location *time.Location) (string, error) {
	ts, err := parseArrowTimestampValue(value, location)
	if err != nil {
		return "", err
	}
	if location != nil {
		ts = ts.In(location)
	}
	return ts.Format("2006-01-02 15:04:05.000000"), nil
}

func formatArrowTimestampTZValue(value interface{}, location *time.Location) (string, error) {
	switch v := value.(type) {
	case decimal128.Num:
		return formatArrowTimestampTZDecimalValue(v)
	}

	ts, err := parseArrowTimestampValue(value, location)
	if err != nil {
		return "", err
	}
	return ts.Format("2006-01-02 15:04:05.000000 -0700"), nil
}

func formatArrowTimestampTZDecimalValue(value decimal128.Num) (string, error) {
	ts := arrowTimestampTZDecimalToTime(value)
	_, offset := ts.Zone()
	zone := time.FixedZone("", offset)
	return ts.In(zone).Format("2006-01-02 15:04:05.000000 -0700"), nil
}

func arrowTimestampTZDecimalToTime(value decimal128.Num) time.Time {
	ts := time.UnixMicro(clampArrowTimestampMicros(int64(value.LowBits())))
	zone := time.FixedZone("", int(int32(value.HighBits())))
	return ts.In(zone)
}

func parseArrowTimestampValue(value interface{}, location *time.Location) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseArrowTimestampString(v, location)
	default:
		return time.Time{}, fmt.Errorf("unsupported arrow timestamp value type %T", value)
	}
}

func parseArrowTimestampString(value string, location *time.Location) (time.Time, error) {
	if location == nil {
		location = time.UTC
	}

	zonedLayouts := []string{
		"2006-01-02 15:04:05.999999999Z0700",
		"2006-01-02 15:04:05.999999Z0700",
		"2006-01-02 15:04:05Z0700",
		time.RFC3339Nano,
	}
	for _, layout := range zonedLayouts {
		if ts, err := time.Parse(layout, value); err == nil {
			return ts, nil
		}
	}

	localLayouts := []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range localLayouts {
		if ts, err := time.ParseInLocation(layout, value, location); err == nil {
			return ts, nil
		}
	}

	return time.Time{}, fmt.Errorf("failed to parse arrow timestamp value %q", value)
}

func clampArrowTimestampMicros(value int64) int64 {
	const (
		arrowTimestampMicrosMin = -377705023201000000
		arrowTimestampMicrosMax = 253402207200000000
	)
	switch {
	case value < arrowTimestampMicrosMin:
		return arrowTimestampMicrosMin
	case value > arrowTimestampMicrosMax:
		return arrowTimestampMicrosMax
	default:
		return value
	}
}

func (c *APIClient) snapshotClientState() func() {
	querySeq := c.QuerySeq
	routeHint := c.routeHint
	nodeID := c.nodeID
	stateRestored := c.stateRestored
	sessionStateRaw := cloneRawMessage(c.sessionStateRaw)
	sessionState := cloneSessionState(c.sessionState)

	return func() {
		c.QuerySeq = querySeq
		c.routeHint = routeHint
		c.nodeID = nodeID
		c.stateRestored = stateRestored
		c.sessionStateRaw = sessionStateRaw
		c.sessionState = sessionState
	}
}

func cloneRawMessage(raw *json.RawMessage) *json.RawMessage {
	if raw == nil {
		return nil
	}
	cloned := json.RawMessage(append([]byte(nil), (*raw)...))
	return &cloned
}

func cloneSessionState(state *SessionState) *SessionState {
	if state == nil {
		return nil
	}

	cloned := *state
	if state.SecondaryRoles != nil {
		roles := append([]string(nil), (*state.SecondaryRoles)...)
		cloned.SecondaryRoles = &roles
	}
	if state.Settings != nil {
		settings := make(map[string]string, len(state.Settings))
		for key, value := range state.Settings {
			settings[key] = value
		}
		cloned.Settings = settings
	}
	return &cloned
}

func formatArrowScalar(value interface{}) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case json.RawMessage:
		return string(v), nil
	case []byte:
		return string(v), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}
