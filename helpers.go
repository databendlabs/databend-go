package godatabend

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	escaper          = strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	dateFormat       = "2006-01-02"
	timeFormat       = "2006-01-02 15:04:05.000000-07:00"
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
