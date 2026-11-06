// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package godatabend

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	ProvisionWarehouseTimeout = "ProvisionWarehouseTimeout"

	ErrDoRequest    = errors.New("DoRequestFailed")
	ErrReadResponse = errors.New("ReadResponseFailed")

	// ErrSessionInvalidated is matched (errors.Is) by every error returned
	// for further queries on an APIClient after the server reported its
	// session state (sticky node, temp tables, transaction) as gone. The
	// concrete value is a *SessionInvalidatedError carrying the cause.
	// Create a new client / connection. Through database/sql it surfaces as
	// driver.ErrBadConn.
	ErrSessionInvalidated = errors.New("databend: session state lost on server, connection must be discarded")
)

// SessionInvalidatedError is returned by an APIClient whose session state was
// reported lost by the server. It keeps the original server error and the
// identifiers needed to find the incident in server logs.
type SessionInvalidatedError struct {
	// Cause is the server error that invalidated the session.
	Cause error
	// SessionID is the session id as known to the server (the session_id
	// cookie, also the query id prefix after login), usable to look the
	// incident up in the server's query/log history.
	SessionID string
	// LostNodeID is the node the session was pinned to when it was lost.
	LostNodeID string
	// Stmts is the number of statements sent on the database/sql connection
	// since it was last taken from the pool, including the failing one. It is
	// 0 when the APIClient is used without database/sql. A value above 1
	// means the caller holds the connection (sql.Conn / sql.Tx): a pooled
	// db.Exec / db.Query runs exactly one statement per checkout.
	Stmts int
}

func (e *SessionInvalidatedError) Error() string {
	var b strings.Builder
	b.WriteString(ErrSessionInvalidated.Error())
	if e.SessionID != "" {
		fmt.Fprintf(&b, "; session_id=%s", e.SessionID)
	}
	if e.LostNodeID != "" {
		fmt.Fprintf(&b, "; lost_node_id=%s", e.LostNodeID)
	}
	if e.Stmts > 0 {
		fmt.Fprintf(&b, "; stmts=%d", e.Stmts)
	}
	if e.Cause != nil {
		fmt.Fprintf(&b, "; cause: %v", e.Cause)
	}
	return b.String()
}

func (e *SessionInvalidatedError) Unwrap() error { return e.Cause }

// Is lets errors.Is(err, ErrSessionInvalidated) match regardless of cause.
func (e *SessionInvalidatedError) Is(target error) bool { return target == ErrSessionInvalidated }

// compactSessionError shortens the sticky rejection: the server appends
// ", request=method=... headers={...}" with a header dump, which is noise on
// every refused call and must not end up in application logs in case a server
// does not mask credentials in it. Other errors are returned unchanged so
// error codes stay matchable.
//
// Only APIError is handled. That is enough because the sticky rejection is
// an HTTP 400 and reaches handleRequestError first as the APIError from
// doRequest / doRequestRaw, which is the call that stores the cause; a
// QueryError carrying the sticky text would only arrive later and is ignored
// (sessionInvalidated is already set). Keep that ordering if this changes.
func compactSessionError(err error) error {
	var apiErr APIError
	if !errors.As(err, &apiErr) {
		return err
	}
	// APIErrorResponseBody is flat, but the server nests the message under
	// "error", so Error() usually falls back to RespText. Cut whichever is used.
	cut := func(s string) string {
		if i := strings.Index(s, ", request="); i > 0 {
			return s[:i]
		}
		return s
	}
	if apiErr.RespBody.Message != "" {
		if short := cut(apiErr.RespBody.Message); short != apiErr.RespBody.Message {
			apiErr.RespBody.Message = short
			return apiErr
		}
		return err
	}
	if short := cut(apiErr.RespText); short != apiErr.RespText {
		apiErr.RespText = short
		return apiErr
	}
	return err
}

type APIErrorResponseBody struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type APIError struct {
	RespBody   APIErrorResponseBody
	RespText   string
	StatusCode int
	Hint       string
}

func (e APIError) Error() string {
	message := e.RespBody.Message
	if message == "" {
		message = e.RespText
	}
	message = fmt.Sprintf("%d %s", e.StatusCode, message)
	if e.Hint != "" {
		message = strings.Trim(message, ".")
		message += ". " + e.Hint
	}
	return message
}

func NewAPIError(hint string, status int, respBuf []byte) error {
	respBody := APIErrorResponseBody{}
	_ = json.Unmarshal(respBuf, &respBody)
	return APIError{
		RespBody:   respBody,
		RespText:   string(respBuf),
		StatusCode: status,
		Hint:       hint,
	}
}

func IsNotFound(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 404
}

func IsProxyErr(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 520
}

func IsAuthFailed(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 401
}

func isBadGateway(err error) bool {
	var apiErr APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == 502
}

// stickyNodeLostMessage is the text Databend returns (HTTP 400) when a request
// carries X-DATABEND-STICKY-NODE pointing to a node that is no longer in the
// cluster. Servers before SessionNodeLost (5113) raise it as BadArguments
// (1006) and json_response rewrites the body to code=400 (the HTTP status),
// message="Some(400) BadArguments. Code: 1006, Text = Sticky session state
// lost: ..., request=...". So for them the message is the only discriminator;
// newer servers are matched by code.
const stickyNodeLostMessage = "Sticky session state lost"

// isStickyNodeLostMessage matches the pre-5113 sticky rejection. The code is
// checked too because error text can embed user SQL (display_with_sql), so a
// statement containing the literal must not invalidate the session.
func isStickyNodeLostMessage(code int, message string) bool {
	switch code {
	case 400, 1006, errCodeSessionNodeLost:
		return strings.Contains(message, stickyNodeLostMessage)
	}
	return false
}

// Server error codes that mean the state the client echoes back in `session`
// (temp tables, last node) no longer exists on the server. Retrying with the
// same session state fails the same way, so the client must drop that state
// and database/sql must discard the connection.
//
// 5110-5112 are raised by fail_to_start_sql, i.e. in the response to POST
// /v1/query before the query gets an id; 5113 comes from the routing
// middleware as HTTP 400.
//
// TransactionTimeout (4003) is deliberately not included. From try_set_txn it
// means the transaction manager expired after idling on a node that is still
// the right one (check_sticky_for_txn ran first and would have raised 5111);
// the server returns txn_state=Fail, temp tables and sticky node stay valid,
// and ROLLBACK recovers the connection. It is also raised by fuse snapshot
// timestamp conflicts of a running query.
const (
	errCodeSessionTimeout      = 5110 // client claims temp tables, server has none left
	errCodeSessionLost         = 5111 // temp tables / txn lived on a node that restarted
	errCodeInvalidSessionState = 5112 // session fields contradict each other
	errCodeSessionNodeLost     = 5113 // sticky node not in cluster (routing middleware)
)

func isSessionInvalidatedCode(code int) bool {
	switch code {
	case errCodeSessionTimeout, errCodeSessionLost, errCodeInvalidSessionState, errCodeSessionNodeLost:
		return true
	}
	return false
}

// nestedQueryError extracts the {"error": {"code": .., "message": ..}} body
// that HTTP-level rejections (4xx) carry. APIErrorResponseBody is flat and
// does not see the nested code.
func nestedQueryError(respText string) *QueryError {
	var body struct {
		Error *QueryError `json:"error"`
	}
	if json.Unmarshal([]byte(respText), &body) != nil {
		return nil
	}
	return body.Error
}

// IsSessionInvalidated reports whether err means the server-side session state
// referenced by this client is gone, so the connection cannot be reused as-is.
// Databend reports this either as HTTP 400 (sticky node unknown, from the
// routing middleware) or as HTTP 200 with an error in the query start response
// body (fail_to_start_sql), so both shapes are checked. Errors of a running
// query (page responses) never match: 5110-5113 are only raised before a query
// starts.
func IsSessionInvalidated(err error) bool {
	if err == nil {
		return false
	}
	var queryErr *QueryError
	if errors.As(err, &queryErr) {
		return isSessionInvalidatedCode(queryErr.Code) ||
			isStickyNodeLostMessage(queryErr.Code, queryErr.Message)
	}
	var apiErr APIError
	if errors.As(err, &apiErr) {
		if apiErr.StatusCode != 400 {
			return false
		}
		if nested := nestedQueryError(apiErr.RespText); nested != nil {
			return isSessionInvalidatedCode(nested.Code) ||
				isStickyNodeLostMessage(nested.Code, nested.Message)
		}
		// Body is not the {"error":{...}} shape (older server or proxy): fall
		// back to the raw text. APIErrorResponseBody is flat while the server
		// nests the message under "error", so RespBody.Message is normally
		// empty and RespText is what actually matches.
		return strings.Contains(apiErr.RespBody.Message, stickyNodeLostMessage) ||
			strings.Contains(apiErr.RespText, stickyNodeLostMessage)
	}
	return false
}

func RespBody(err error) APIErrorResponseBody {
	var apiErr APIError
	if !errors.As(err, &apiErr) {
		return APIErrorResponseBody{}
	}
	return apiErr.RespBody
}
