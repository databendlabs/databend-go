package godatabend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeHeaders(t *testing.T) {
	c := APIClient{
		User:      "root",
		Password:  "root",
		Host:      "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
		Tenant:    "tn3ftqihs",
		Warehouse: "bl",
	}
	headers := c.makeHeaders()
	assert.Equal(t, headers["Authorization"], []string{"Basic cm9vdDpyb290"})
	assert.Equal(t, headers["X-Databend-Tenant"], []string{"tn3ftqihs"})
	assert.Equal(t, headers["X-Databend-Warehouse"], []string{"bl"})
}
