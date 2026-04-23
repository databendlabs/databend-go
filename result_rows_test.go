package godatabend

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeJSONResponseMaterializesTypedRows(t *testing.T) {
	resp := QueryResponse{
		ID: "json-query",
		Settings: &Settings{
			TimeZone: "Asia/Shanghai",
		},
		Schema: &[]DataField{
			{Name: "n", Type: "Int32"},
			{Name: "name", Type: "String"},
			{Name: "ts", Type: "Timestamp"},
		},
		Data: [][]*string{{
			strPtr("7"),
			strPtr("alice"),
			strPtr("2025-01-16 10:01:26.739219"),
		}},
	}

	body, err := json.Marshal(resp)
	require.NoError(t, err)

	decoded, err := decodeQueryResponse(&rawHTTPResponse{
		headers: http.Header{contentType: []string{jsonContentType}},
		body:    body,
	})
	require.NoError(t, err)
	require.Len(t, decoded.typedRows, 1)
	require.Len(t, decoded.typedRows[0], 3)

	assert.Equal(t, "7", decoded.typedRows[0][0])
	assert.Equal(t, "alice", decoded.typedRows[0][1])

	ts, ok := decoded.typedRows[0][2].(time.Time)
	require.True(t, ok)
	loc, err := time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2025, 1, 16, 10, 1, 26, 739219000, loc), ts)
}
