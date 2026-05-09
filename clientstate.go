package godatabend

import (
	"encoding/json"
	"net/http"
)

func (c *APIClient) snapshotClientState() func() {
	querySeq := c.QuerySeq
	routeHint := c.routeHint
	nodeID := c.nodeID
	stateRestored := c.stateRestored
	sessionStateRaw := cloneRawMessage(c.sessionStateRaw)
	sessionState := cloneSessionState(c.sessionState)

	return func() {
		c.QuerySeq = querySeq
		c.routeHint = routeHint
		c.nodeID = nodeID
		c.stateRestored = stateRestored
		c.sessionStateRaw = sessionStateRaw
		c.sessionState = sessionState
	}
}

func cloneRawMessage(raw *json.RawMessage) *json.RawMessage {
	if raw == nil {
		return nil
	}
	cloned := json.RawMessage(append([]byte(nil), (*raw)...))
	return &cloned
}

func cloneSessionState(state *SessionState) *SessionState {
	if state == nil {
		return nil
	}

	cloned := *state
	if state.SecondaryRoles != nil {
		roles := append([]string(nil), (*state.SecondaryRoles)...)
		cloned.SecondaryRoles = &roles
	}
	if state.Settings != nil {
		settings := make(map[string]string, len(state.Settings))
		for key, value := range state.Settings {
			settings[key] = value
		}
		cloned.Settings = settings
	}
	return &cloned
}

type APIClientState struct {
	SessionID    string
	QuerySeq     int64
	RouteHint    string
	NodeID       string
	SessionState string
	Cookies      map[string]*http.Cookie
}

func (c *APIClient) WithState(state *APIClientState) *APIClient {
	if state == nil {
		return c
	}
	c.stateRestored = true
	c.SessionID = state.SessionID
	c.QuerySeq = state.QuerySeq
	c.routeHint = state.RouteHint
	c.nodeID = state.NodeID
	for name, cookie := range state.Cookies {
		c.cli.Jar.SetCookies(nil, []*http.Cookie{{Name: name, Value: cookie.Value}})
	}
	if state.SessionState != "" {
		var sessionStateRaw json.RawMessage
		var sessionState SessionState
		err := json.Unmarshal([]byte(state.SessionState), &sessionStateRaw)
		if err != nil {
			return c
		}
		c.sessionStateRaw = &sessionStateRaw
		err = json.Unmarshal([]byte(state.SessionState), &sessionState)
		if err != nil {
			return c
		}
		c.sessionState = &sessionState
	}
	return c
}

func (c *APIClient) GetState() *APIClientState {
	var sessionStateStr string
	if c.sessionStateRaw != nil {
		sessionStateJson, _ := c.sessionStateRaw.MarshalJSON()
		sessionStateStr = string(sessionStateJson)
	}
	cookies := make(map[string]*http.Cookie)
	for _, cookie := range c.cli.Jar.Cookies(nil) {
		cookies[cookie.Name] = cookie
	}
	return &APIClientState{
		SessionID:    c.SessionID,
		QuerySeq:     c.QuerySeq,
		RouteHint:    c.routeHint,
		NodeID:       c.nodeID,
		SessionState: sessionStateStr,
		Cookies:      cookies,
	}
}
