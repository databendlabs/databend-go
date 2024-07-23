package godatabend

import (
	"encoding/json"
	"fmt"
	"strings"
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
	ID      string           `json:"id"`
	NodeID  string           `json:"node_id"`
	Session *json.RawMessage `json:"session"`
	Schema  *[]DataField     `json:"schema"`
	Data    [][]string       `json:"data"`
	State   string           `json:"state"`
	Error   *QueryError      `json:"error"`
	Stats   *QueryStats      `json:"stats"`
	// TODO: Affect rows
	StatsURI string `json:"stats_uri"`
	FinalURI string `json:"final_uri"`
	NextURI  string `json:"next_uri"`
	KillURI  string `json:"kill_uri"`
}

func (r *QueryResponse) ReadFinished() bool {
	return r.NextURI == "" || strings.Contains(r.NextURI, "/final")
}

type QueryStats struct {
	RunningTimeMS  float64       `json:"running_time_ms"`
	ScanProgress   QueryProgress `json:"scan_progress"`
	WriteProgress  QueryProgress `json:"write_progress"`
	ResultProgress QueryProgress `json:"result_progress"`
}

// QueryStatsTracker is a function that will be called when query stats are updated,
// it can be specified in the Config struct.
type QueryStatsTracker func(queryID string, stats *QueryStats)

type QueryProgress struct {
	Bytes uint64 `json:"bytes"`
	Rows  uint64 `json:"rows"`
}

type QueryRequest struct {
	// We use client session instead of server session with session_id
	// SessionID  string            `json:"session_id,omitempty"`

	Session    *json.RawMessage  `json:"session,omitempty"`
	SQL        string            `json:"sql"`
	Pagination *PaginationConfig `json:"pagination,omitempty"`

	// Default to true
	// StringFields  bool  `json:"string_fields,omitempty"`

	StageAttachment *StageAttachmentConfig `json:"stage_attachment,omitempty"`
}

type QueryIDGenerator func() string

type PaginationConfig struct {
	WaitTime        int64 `json:"wait_time_secs,omitempty"`
	MaxRowsInBuffer int64 `json:"max_rows_in_buffer,omitempty"`
	MaxRowsPerPage  int64 `json:"max_rows_per_page,omitempty"`
}

type TxnState string

const (
	TxnStateActive     TxnState = "Active"
	TxnStateAutoCommit TxnState = "AutoCommit"
)

type SessionState struct {
	Database       string    `json:"database,omitempty"`
	Role           string    `json:"role,omitempty"`
	SecondaryRoles *[]string `json:"secondary_roles,omitempty"`

	// Since we use client session, this should not be used
	// KeepServerSessionSecs uint64            `json:"keep_server_session_secs,omitempty"`

	Settings map[string]string `json:"settings,omitempty"`

	// txn
	TxnState TxnState `json:"txn_state,omitempty"` // "Active", "AutoCommit"
}

type StageAttachmentConfig struct {
	Location          string            `json:"location"`
	FileFormatOptions map[string]string `json:"file_format_options,omitempty"`
	CopyOptions       map[string]string `json:"copy_options,omitempty"`
}

type ServerInfo struct {
	Id        string `json:"id"`
	StartTime string `json:"start_time"`
}
