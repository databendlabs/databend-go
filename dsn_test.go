package godatabend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatDSN(t *testing.T) {
	dsn := "https://username:password@app.databend.com:443/test?warehouse=wh&org=databend&timeout=1s&idle_timeout=2s&tls_config=tls-settings"
	cfg, err := ParseDSN(dsn)
	if assert.NoError(t, err) {
		dsn2 := cfg.FormatDSN()
		assert.Equal(t, len(dsn), len(dsn2))
		assert.Contains(t, dsn2, "warehouse=wh")
		assert.Contains(t, dsn2, "org=databend")
		assert.Contains(t, dsn2, "https://username:password@app.databend.com:443/test?")
		assert.Contains(t, dsn2, "timeout=1s")
		assert.Contains(t, dsn2, "idle_timeout=2s")
		assert.Contains(t, dsn2, "tls_config=tls-settings")
	}
}

func TestConfigURL(t *testing.T) {
	dsn := "https://username:password@app.databend.com:443/test?warehouse=wh&org=databend&timeout=1s&idle_timeout=2s&tls_config=tls-settings"
	cfg, err := ParseDSN(dsn)
	if assert.NoError(t, err) {
		u1 := cfg.url(nil, true).String()
		t.Log(u1)
		assert.Equal(t, "https://username:password@app.databend.com:443/test", u1)
		u2 := cfg.url(map[string]string{"default_format": "Native"}, false).String()
		assert.Contains(t, u2, "https://username:password@app.databend.com:443/?")
		assert.Contains(t, u2, "default_format=Native")
		assert.Contains(t, u2, "database=test")
	}
}

func TestParseDSN(t *testing.T) {
	dsn := "https://username:password@app.databend.com:443/test?warehouse=wh&org=databend&timeout=1s&idle_timeout=2s&tls_config=tls-settings"
	cfg, err := ParseDSN(dsn)
	if assert.NoError(t, err) {
		assert.Equal(t, "username", cfg.User)
		assert.Equal(t, "password", cfg.Password)
		assert.Equal(t, "https", cfg.Scheme)
		assert.Equal(t, "wh", cfg.Warehouse)
		assert.Equal(t, "databend", cfg.Org)
		assert.Equal(t, "app.databend.com:443", cfg.Host)
		assert.Equal(t, "test", cfg.Database)
		assert.Equal(t, "tls-settings", cfg.TLSConfig)
		assert.Equal(t, time.Second, cfg.Timeout)
	}
}
