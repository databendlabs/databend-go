package godatabend

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDSNWithEncodedChars(t *testing.T) {
	username := "test_user"
	password := "pa$$?word:abc@123"
	dsn := fmt.Sprintf("databend+https://%s:%s@host:443/db?param1=value1&param2=value2", url.QueryEscape(username), url.QueryEscape(password))
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	assert.Equal(t, username, cfg.User)
	assert.Equal(t, password, cfg.Password)
	assert.Equal(t, "host:443", cfg.Host)
	assert.Equal(t, "db", cfg.Database)
}
func TestParseDSNWithSpecialChars(t *testing.T) {
	dsn := "databend+https://use%#$^&r:pa$$?word@host:443/db?param1=value1&param2=value2"
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	assert.Equal(t, "use%#$^&r", cfg.User)
	assert.Equal(t, "pa$$?word", cfg.Password)
	assert.Equal(t, "host:443", cfg.Host)
	assert.Equal(t, "db", cfg.Database)
	assert.Equal(t, "value1", cfg.Params["param1"])
	assert.Equal(t, "value2", cfg.Params["param2"])
}

func TestFormatDSN(t *testing.T) {
	dsn := "databend+https://username:password@tn3ftqihs.ch.aws-us-east-2.default.databend.com/test?role=test_role&empty_field_as=null&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&warehouse=wh&sessionParam1=sessionValue1"
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	assert.Equal(t, "wh", cfg.Warehouse)
	assert.Equal(t, "tn3ftqihs.ch.aws-us-east-2.default.databend.com:443", cfg.Host)
	assert.Equal(t, "test", cfg.Database)
	assert.Equal(t, "tls-settings", cfg.TLSConfig)
	assert.Equal(t, time.Second, cfg.Timeout)
	assert.Equal(t, int64(10000), cfg.MaxRowsPerPage)
	assert.Equal(t, int64(10), cfg.WaitTimeSecs)
	assert.Equal(t, int64(5000000), cfg.MaxRowsInBuffer)
	assert.Equal(t, "test_role", cfg.Role)
	assert.Equal(t, "sessionValue1", cfg.Params["sessionParam1"])

	dsn1 := cfg.FormatDSN()
	cfg1, err := ParseDSN(dsn1)
	require.NoError(t, err)
	assert.Equal(t, cfg, cfg1)
}

func TestParseDSNWithParams(t *testing.T) {
	// Create a new Config with some params
	cfg := NewConfig()
	cfg.Params["param1"] = "value1"
	cfg.Params["param2"] = "value2"

	// Generate DSN string
	dsn := cfg.FormatDSN()

	// Parse the DSN string
	parsedCfg, err := ParseDSN(dsn)
	require.NoError(t, err)

	// Check that the parsed Config includes the params
	assert.Equal(t, "value1", parsedCfg.Params["param1"])
	assert.Equal(t, "value2", parsedCfg.Params["param2"])
}

func TestFormatDSNWithParams(t *testing.T) {
	// Create a new Config with some params
	cfg := NewConfig()
	cfg.Params["param1"] = "value1"
	cfg.Params["param2"] = "value2"

	// Call FormatDSN
	dsn := cfg.FormatDSN()

	// Check that the DSN includes the params
	assert.Contains(t, dsn, "param1=value1")
	assert.Contains(t, dsn, "param2=value2")
}

func TestParseDSN(t *testing.T) {
	t.Run("test simple dns parse", func(t *testing.T) {
		dsn := "databend+http://app.databend.com:8000/test?tenant=tn&warehouse=wh&timeout=1s&wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&tls_config=tls-settings&access_token_file=/tmp/file1"

		cfg, err := ParseDSN(dsn)
		assert.Equal(t, "string", cfg.EmptyFieldAs)
		require.NoError(t, err)
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
			require.NoError(t, err)

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
