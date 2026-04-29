package godatabend

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

type binaryOutputFormat uint8

const (
	binaryOutputFormatHex binaryOutputFormat = iota
	binaryOutputFormatBase64
	binaryOutputFormatUTF8
	binaryOutputFormatUTF8Lossy
)

type httpJSONResultMode uint8

const (
	httpJSONResultModeDriver httpJSONResultMode = iota
	httpJSONResultModeDisplay
)

func parseBinaryOutputFormat(s string) binaryOutputFormat {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "BASE64":
		return binaryOutputFormatBase64
	case "UTF-8", "UTF8":
		return binaryOutputFormatUTF8
	case "UTF-8-LOSSY", "UTF8-LOSSY":
		return binaryOutputFormatUTF8Lossy
	default:
		return binaryOutputFormatHex
	}
}

func parseHTTPJSONResultMode(s string) httpJSONResultMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "display":
		return httpJSONResultModeDisplay
	default:
		return httpJSONResultModeDriver
	}
}

func materializeBinaryFromString(value string, format binaryOutputFormat, mode httpJSONResultMode) (driver.Value, error) {
	// Databend HTTP API uses driver mode by default, where Binary cells are encoded as hex
	// regardless of binary_output_format.
	if mode != httpJSONResultModeDisplay {
		raw, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode binary hex value: %w", err)
		}
		return raw, nil
	}

	switch format {
	case binaryOutputFormatBase64:
		raw, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode binary base64 value: %w", err)
		}
		return raw, nil
	case binaryOutputFormatUTF8, binaryOutputFormatUTF8Lossy:
		return []byte(value), nil
	default:
		raw, err := hex.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode binary hex value: %w", err)
		}
		return raw, nil
	}
}

func materializeBinaryFromBinary(value []byte) driver.Value {
	return append([]byte(nil), value...)
}
