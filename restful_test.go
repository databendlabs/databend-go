package godatabend

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeHeadersUserPassword(t *testing.T) {
	c := APIClient{
		user:     "root",
		password: "root",
		host:     "localhost:8000",
		tenant:   "default",
		role:     "role1",
	}
	headers, err := c.makeHeaders(context.TODO())
	assert.Nil(t, err)
	assert.Equal(t, headers["Authorization"], []string{"Basic cm9vdDpyb290"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"default"})
	session := c.getSessionConfig()
	assert.Equal(t, session.Role, "role1")
}

func TestMakeHeadersAccessToken(t *testing.T) {
	c := APIClient{
		host:              "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		tenant:            "tn3ftqihs",
		accessTokenLoader: NewStaticAccessTokenLoader("abc123"),
		warehouse:         "small-abc",
	}
	headers, err := c.makeHeaders(context.TODO())
	assert.Nil(t, err)
	assert.Equal(t, headers["Authorization"], []string{"Bearer abc123"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"tn3ftqihs"})
	assert.Equal(t, headers["X-Databend-Warehouse"], []string{"small-abc"})
}

func TestDoQuery(t *testing.T) {
	var result QueryResponse
	result.ID = "mockid1"
	mockDoRequest := func(method, path string, req interface{}, resp interface{}) error {
		buf, _ := json.Marshal(result)
		json.Unmarshal(buf, resp)
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
	_, err := c.DoQuery(context.Background(), "SELECT 1", []driver.Value{})
	assert.NoError(t, err)
	assert.Equal(t, gotQueryID, "mockid1")
}
