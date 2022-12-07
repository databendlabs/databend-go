package godatabend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeHeadersUserPassword(t *testing.T) {
	c := APIClient{
		User:     "root",
		Password: "root",
		Host:     "localhost:8000",
		Tenant:   "default",
	}
	headers := c.makeHeaders()
	assert.Equal(t, headers["Authorization"], []string{"Basic cm9vdDpyb290"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"default"})
}

func TestMakeHeadersAccessToken(t *testing.T) {
	c := APIClient{
		Host:        "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		Tenant:      "tn3ftqihs",
		AccessToken: "abc123",
		Warehouse:   "small-abc",
	}
	headers := c.makeHeaders()
	assert.Equal(t, headers["Authorization"], []string{"Bearer abc123"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"tn3ftqihs"})
	assert.Equal(t, headers["X-Databend-Warehouse"], []string{"small-abc"})
}
