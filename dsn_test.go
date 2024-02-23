package godatabend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatDSN(t *testing.T) {
	dsn := "databend+https://username:password@tn3ftqihs.ch.aws-us-east-2.default.databend.com/test?role=test_role&empty_field_as=string&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&warehouse=wh"
	cfg, err := ParseDSN(dsn)
	require.Nil(t, err)

	assert.Equal(t, "wh", cfg.Warehouse)
	assert.Equal(t, "tn3ftqihs.ch.aws-us-east-2.default.databend.com:443", cfg.Host)
	assert.Equal(t, "test", cfg.Database)
	assert.Equal(t, "tls-settings", cfg.TLSConfig)
	assert.Equal(t, time.Second, cfg.Timeout)
	assert.Equal(t, int64(10000), cfg.MaxRowsPerPage)
	assert.Equal(t, int64(10), cfg.WaitTimeSecs)
	assert.Equal(t, int64(5000000), cfg.MaxRowsInBuffer)
	assert.Equal(t, "test_role", cfg.Role)

	dsn1 := cfg.FormatDSN()
	cfg1, err := ParseDSN(dsn1)
	require.Nil(t, err)
	assert.Equal(t, cfg, cfg1)
}

func TestParseDSN(t *testing.T) {
	t.Run("test simple dns parse", func(t *testing.T) {
		dsn := "databend+http://app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&access_token_file=/tmp/file1"

		cfg, err := ParseDSN(dsn)
		require.Nil(t, err)
		assert.Equal(t, "/tmp/file1", cfg.AccessTokenFile)
	})

	t.Run("test parse dsn with different protocols", func(t *testing.T) {
		tests := []string{
			"databend+http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&",
			"db+http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings",
			"bend+http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings",
			"http://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings",
			"databend://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&sslmode=disable",
			"db://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&sslmode=disable",
			"dd://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&sslmode=disable",
			"bend://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&sslmode=disable",
			"https://username:password@app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&sslmode=disable",
		}

		for _, test := range tests {
			cfg, err := ParseDSN(test)
			require.Nil(t, err)

			assert.Equal(t, "username", cfg.User)
			assert.Equal(t, "password", cfg.Password)
			assert.Equal(t, "tn", cfg.Tenant)
			assert.Equal(t, "wh", cfg.Warehouse)
			assert.Equal(t, "app.databend.com:8000", cfg.Host)
			assert.Equal(t, "test", cfg.Database)
			assert.Equal(t, "tls-settings", cfg.TLSConfig)
			assert.Equal(t, SSL_MODE_DISABLE, cfg.SSLMode)
			assert.Equal(t, time.Second, cfg.Timeout)
			assert.Equal(t, int64(10), cfg.WaitTimeSecs)
			assert.Equal(t, int64(5000000), cfg.MaxRowsInBuffer)
		}
	})
}
