package godatabend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeHeadersUserPassword(t *testing.T) {
	c := APIClient{
		user:     "root",
		password: "root",
		host:     "localhost:8000",
		tenant:   "default",
	}
	headers, err := c.makeHeaders()
	assert.Nil(t, err)
	assert.Equal(t, headers["Authorization"], []string{"Basic cm9vdDpyb290"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"default"})
}

func TestMakeHeadersAccessToken(t *testing.T) {
	c := APIClient{
		host:        "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		tenant:      "tn3ftqihs",
		accessToken: "abc123",
		warehouse:   "small-abc",
	}
	headers, err := c.makeHeaders()
	assert.Nil(t, err)
	assert.Equal(t, headers["Authorization"], []string{"Bearer abc123"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"tn3ftqihs"})
	assert.Equal(t, headers["X-Databend-Warehouse"], []string{"small-abc"})
}
