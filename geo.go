package godatabend

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strings"
)

type geoOutputFormat uint8

const (
	geoOutputFormatGeoJSON geoOutputFormat = iota
	geoOutputFormatWKB
	geoOutputFormatWKT
	geoOutputFormatEWKB
	geoOutputFormatEWKT
)

func parseGeoOutputFormat(s string) geoOutputFormat {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "WKB":
		return geoOutputFormatWKB
	case "WKT":
		return geoOutputFormatWKT
	case "EWKB":
		return geoOutputFormatEWKB
	case "EWKT":
		return geoOutputFormatEWKT
	default:
		return geoOutputFormatGeoJSON
	}
}

func (f geoOutputFormat) returnsBinary() bool {
	return f == geoOutputFormatWKB || f == geoOutputFormatEWKB
}

func materializeGeoFromString(kind, value string, format geoOutputFormat) (driver.Value, error) {
	if !format.returnsBinary() {
		return value, nil
	}

	raw, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode %s hex value: %w", strings.ToLower(kind), err)
	}
	return raw, nil
}

func materializeGeoFromBinary(_ string, value []byte, format geoOutputFormat) (driver.Value, error) {
	if format.returnsBinary() {
		return append([]byte(nil), value...), nil
	}
	return string(value), nil
}
