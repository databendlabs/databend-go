package godatabend

import (
	"encoding/hex"
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

func TestDecodeJSONResponseMaterializesGeoRows(t *testing.T) {
	wkb, err := hex.DecodeString("01010000000000000000004E400000000000804240")
	require.NoError(t, err)

	testCases := []struct {
		name     string
		typ      string
		settings *Settings
		input    string
		want     any
	}{
		{
			name:     "geometry-wkb",
			typ:      "Geometry",
			settings: &Settings{GeometryOutputFormat: "WKB"},
			input:    "01010000000000000000004E400000000000804240",
			want:     wkb,
		},
		{
			name:     "geography-geojson",
			typ:      "Geography",
			settings: &Settings{GeometryOutputFormat: "GEOJSON"},
			input:    `{"type":"Point","coordinates":[60,37]}`,
			want:     `{"type":"Point","coordinates":[60,37]}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := QueryResponse{
				ID:       "json-geo-query",
				Settings: tc.settings,
				Schema:   &[]DataField{{Name: "g", Type: tc.typ}},
				Data:     [][]*string{{strPtr(tc.input)}},
			}

			body, err := json.Marshal(resp)
			require.NoError(t, err)

			decoded, err := decodeQueryResponse(&rawHTTPResponse{
				headers: http.Header{contentType: []string{jsonContentType}},
				body:    body,
			})
			require.NoError(t, err)
			require.Len(t, decoded.typedRows, 1)
			require.Len(t, decoded.typedRows[0], 1)
			assert.Equal(t, tc.want, decoded.typedRows[0][0])
		})
	}
}
