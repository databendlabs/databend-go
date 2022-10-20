package godatabend

type QueryError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Kind    string `json:"kind"`
}

type DataField struct {
	Name     string     `json:"name"`
	DataType TypeDetail `json:"data_type"`
}

type TypeDetail struct {
	Type string `json:"type"`
}

type DataSchema struct {
	Fields []DataField `json:"fields"`
}

type QueryResponse struct {
	Data     [][]interface{} `json:"data"`
	Error    *QueryError     `json:"error"`
	FinalURI string          `json:"final_uri"`
	Id       string          `json:"id"`
	NextURI  string          `json:"next_uri"`
	Schema   DataSchema      `json:"schema,omitempty"`
	State    string          `json:"state"`
	Stats    QueryStats      `json:"stats"`
	StatsURI string          `json:"stats_uri"`
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
	SQL string `json:"sql"`
}
