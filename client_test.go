package godatabend

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeHeadersUserPassword(t *testing.T) {
	c := APIClient{
		user:         "root",
		password:     "root",
		host:         "localhost:8000",
		tenant:       "default",
		sessionState: &SessionState{Role: "role1"},
	}
	headers, err := c.makeHeaders(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []string{"Basic cm9vdDpyb290"}, headers["Authorization"])
	assert.Equal(t, []string{"default"}, headers["X-Databend-Tenant"])
	session := c.getSessionState()
	assert.Equal(t, "role1", session.Role)
}

func TestMakeHeadersAccessToken(t *testing.T) {
	c := APIClient{
		host:              "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		tenant:            "tn3ftqihs",
		accessTokenLoader: NewStaticAccessTokenLoader("abc123"),
		warehouse:         "small-abc",
	}
	ctx := checkQueryID(context.Background())
	headers, err := c.makeHeaders(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"Bearer abc123"}, headers["Authorization"])
	assert.Equal(t, []string{"tn3ftqihs"}, headers["X-Databend-Tenant"])
	assert.Equal(t, []string{"small-abc"}, headers["X-Databend-Warehouse"])
	assert.NotEmptyf(t, headers["X-Databend-Query-Id"], "Query ID is not empty")
}

func TestCheckQueryID(t *testing.T) {
	// Test case 1: Context does not have ContextKeyQueryID
	ctx := context.Background()
	newCtx := checkQueryID(ctx)
	_, ok := newCtx.Value(ContextKeyQueryID).(string)
	assert.True(t, ok, "Expected ContextKeyQueryID to be present in the context")

	// Test case 2: Context already has ContextKeyQueryID
	queryID := uuid.NewString()
	ctxWithQueryID := context.WithValue(ctx, ContextKeyQueryID, queryID)
	newCtxWithQueryID := checkQueryID(ctxWithQueryID)
	assert.Equal(t, queryID, newCtxWithQueryID.Value(ContextKeyQueryID), "Expected ContextKeyQueryID to remain unchanged in the context")
}

func TestMakeHeadersQueryID(t *testing.T) {
	c := APIClient{
		user:         "root",
		password:     "root",
		host:         "localhost:8000",
		tenant:       "default",
		sessionState: &SessionState{Role: "role1"},
	}
	queryId := uuid.NewString()
	ctx := context.WithValue(context.Background(), ContextKeyQueryID, queryId)
	headers, err := c.makeHeaders(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{queryId}, headers["X-Databend-Query-Id"])
}

func TestDoQuery(t *testing.T) {
	var result QueryResponse
	result.ID = "mockid1"
	result.Stats = new(QueryStats)
	mockDoRequest := func(method, path string, req interface{}, resp interface{}) error {
		buf, _ := json.Marshal(result)
		_ = json.Unmarshal(buf, resp)
		return nil
	}

	var gotQueryID string
	statsTracker := func(queryID string, stats *QueryStats) {
		gotQueryID = queryID
	}

	c := APIClient{
		host:              "tnxxxxxxx.gw.aws-us-east-2.default.databend.com",
		tenant:            "tnxxxxxxx",
		accessTokenLoader: NewStaticAccessTokenLoader("abc123"),
		warehouse:         "small-abc",
		doRequestFunc:     mockDoRequest,
		statsTracker:      statsTracker,
	}
	queryId := "mockid1"
	ctx := context.WithValue(context.Background(), ContextKeyQueryID, queryId)
	resp, err := c.StartQuery(ctx, "SELECT 1", nil, []driver.Value{})
	assert.NoError(t, err)
	assert.Equal(t, "mockid1", gotQueryID)
	assert.Equal(t, resp.ID, queryId)
}
