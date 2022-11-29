package godatabend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeHeaders(t *testing.T) {
	c := APIClient{
		User:     "root",
		Password: "root",
		Host:     "tn3ftqihs--bl.ch.aws-us-east-2.default.databend.com",
	}
	headers := c.makeHeaders()
	assert.Equal(t, headers["Authorization"], []string{"Basic cm9vdDpyb290"})
	// assert.Equal(t, headers["X-Databendcloud-Tenant"], []string{"tn3ftqihs"})
	// assert.Equal(t, headers["X-Databendcloud-Warehouse"], []string{"bl"})

}
