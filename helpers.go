package godatabend

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"net/http"
	"strings"
	"time"
)

var (
	escaper          = strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	dateFormat       = "2006-01-02"
	timeFormat       = "2006-01-02 15:04:05"
	dateTime64Format = "2006-01-02 15:04:05.999999999"
)

func escape(s string) string {
	return escaper.Replace(s)
}

func quote(s string) string {
	return "'" + s + "'"
}

func formatTime(value time.Time) string {
	return quote(value.Format(timeFormat))
}

func formatDate(value time.Time) string {
	return quote(value.Format(dateFormat))
}

func readResponse(response *http.Response) (result []byte, err error) {
	if response.ContentLength > 0 {
		result = make([]byte, 0, response.ContentLength)
	}
	buf := bytes.NewBuffer(result)
	defer response.Body.Close()
	_, err = buf.ReadFrom(response.Body)
	result = buf.Bytes()
	return
}

func getTableFromInsertQuery(query string) (string, error) {
	if !strings.Contains(query, "insert") && !strings.Contains(query, "INSERT") {
		return "", errors.New("wrong insert statement")
	}
	splitQuery := strings.Split(query, " ")
	if len(splitQuery) > 2 {
		return strings.TrimSpace(splitQuery[2]), nil
	}
	return "", errors.New("wrong insert")
}

func generateDescTable(query string) (string, error) {
	table, err := getTableFromInsertQuery(query)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("DESC %s", table), nil
}

func databendParquetReflect(databendType string) string {

	var parquetTyep string
	switch databendType {
	case "VARCHAR":
		parquetTyep = "type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"

	case "BOOLEAN":
		parquetTyep = "type=BOOLEAN"
	case "TINYINT", "SMALLINT", "INT":
		parquetTyep = "type=INT32"
	case "BIGINT":
		parquetTyep = "type=INT64"
	case "FLOAT":
		parquetTyep = "type=FLOAT"
	case "DOUBLE":
		parquetTyep = "type=DOUBLE"
	case "DATE":
		parquetTyep = "type=INT32, convertedtype=DATE"
	case "TIMESTAMP":
		parquetTyep = "type=INT64"
	case "ARRAY":
		parquetTyep = "type=LIST, convertedtype=LIST"

	}
	return parquetTyep
}
