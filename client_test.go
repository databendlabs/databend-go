package godatabend

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)
	assert.Equal(t, headers["Authorization"], []string{"Basic cm9vdDpyb290"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"default"})
	session := c.getSessionState()
	assert.Equal(t, session.Role, "role1")
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
	assert.Nil(t, err)
	assert.Equal(t, headers["Authorization"], []string{"Bearer abc123"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"tn3ftqihs"})
	assert.Equal(t, headers["X-Databend-Warehouse"], []string{"small-abc"})
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
	assert.Nil(t, err)
	assert.Equal(t, headers["X-Databend-Query-Id"], []string{queryId})
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
		host:              "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		tenant:            "tn3ftqihs",
		accessTokenLoader: NewStaticAccessTokenLoader("abc123"),
		warehouse:         "small-abc",
		doRequestFunc:     mockDoRequest,
		statsTracker:      statsTracker,
	}
	queryId := "mockid1"
	ctx := context.WithValue(context.Background(), ContextKeyQueryID, queryId)
	resp, err := c.StartQuery(ctx, "SELECT 1", []driver.Value{})
	assert.NoError(t, err)
	assert.Equal(t, gotQueryID, "mockid1")
	assert.Equal(t, resp.ID, queryId)
}
