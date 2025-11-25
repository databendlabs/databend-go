package godatabend

import (
	"encoding/json"
	"net/http"
)

type APIClientState struct {
	SessionID    string
	RouteHint    string
	NodeID       string
	SessionState string
	Cookies      map[string]*http.Cookie
}

func (c *APIClient) WithState(state *APIClientState) *APIClient {
	if state == nil {
		return c
	}
	c.SessionID = state.SessionID
	c.routeHint = state.RouteHint
	c.nodeID = state.NodeID
	for name, cookie := range state.Cookies {
		c.cli.Jar.SetCookies(nil, []*http.Cookie{{Name: name, Value: cookie.Value}})
	}
	if state.SessionState != "" {
		var sessionState SessionState
		err := json.Unmarshal([]byte(state.SessionState), &sessionState)
		if err != nil {
			return c
		}
		c.sessionState = &sessionState
		sessionStateRawJson, _ := json.Marshal(sessionState)
		sessionStateRaw := json.RawMessage(sessionStateRawJson)
		c.sessionStateRaw = &sessionStateRaw
	}
	return c
}

func (c *APIClient) GetState() *APIClientState {
	state := &APIClientState{
		SessionID:    c.SessionID,
		RouteHint:    c.routeHint,
		NodeID:       c.nodeID,
		SessionState: "",
		Cookies:      make(map[string]*http.Cookie),
	}
	if c.sessionState != nil {
		sessionStateJson, _ := json.Marshal(c.sessionState)
		state.SessionState = string(sessionStateJson)
	}
	for _, cookie := range c.cli.Jar.Cookies(nil) {
		state.Cookies[cookie.Name] = cookie
	}
	return state
}
