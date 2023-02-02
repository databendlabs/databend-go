package godatabend

import (
	"fmt"
	"time"
)

type QueryError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Kind    string `json:"kind"`
}

func (e *QueryError) Error() string {
	text := fmt.Sprintf("code: %d", e.Code)
	if e.Message != "" {
		text += fmt.Sprintf(", message: %s", e.Message)
	}
	if e.Kind != "" {
		text += fmt.Sprintf(", kind: %s", e.Kind)
	}
	return text
}

type DataField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type QueryResponse struct {
	Data     [][]string  `json:"data"`
	Error    *QueryError `json:"error"`
	FinalURI string      `json:"final_uri"`
	Id       string      `json:"id"`
	NextURI  string      `json:"next_uri"`
	Schema   []DataField `json:"schema"`
	State    string      `json:"state"`
	Stats    QueryStats  `json:"stats"`
	StatsURI string      `json:"stats_uri"`
}

type QueryStats struct {
	RunningTimeMS float64       `json:"running_time_ms"`
	ScanProgress  QueryProgress `json:"scan_progress"`
}

type QueryProgress struct {
	Bytes uint64 `json:"bytes"`
	Rows  uint64 `json:"rows"`
}

type QueryRequest struct {
	SQL        string `json:"sql"`
	Pagination `json:"pagination"`
}

type Pagination struct {
	WaitTime time.Duration `json:"wait_time_secs" default:"60"`
}
