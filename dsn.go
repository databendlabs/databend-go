package godatabend

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultDomain    = "app.databend.com"
	defaultScheme    = "databend"
	SSL_MODE_DISABLE = "disable"
)

// Config is a set of configuration parameters
type Config struct {
	Tenant    string // Tenant
	Warehouse string // Warehouse
	User      string // Username
	Password  string // Password (requires User)
	Database  string // Database name

	Role string // Role is the databend role you want to use for the current connection

	AccessToken       string
	AccessTokenFile   string // path to file containing access token, it can be used to rotate access token
	AccessTokenLoader AccessTokenLoader

	Host    string
	Timeout time.Duration
	/* Pagination params: WaitTimeSecs,  MaxRowsInBuffer, MaxRowsPerPage
	Pagination: critical conditions for each HTTP request to return (before all remaining result is ready to return)
	Related docs:https://databend.rs/doc/integrations/api/rest#query-request
	*/
	WaitTimeSecs    int64
	MaxRowsInBuffer int64
	MaxRowsPerPage  int64
	Location        *time.Location
	Debug           bool
	GzipCompression bool
	Params          map[string]string
	TLSConfig       string
	SSLMode         string

	// track the progress of query execution
	StatsTracker QueryStatsTracker

	// used on the storage which does not support presigned url like HDFS, local fs
	PresignedURLDisabled bool

	// Specifies the value that should be used when encountering empty fields, including both ,, and ,"",, in the CSV data being loaded into the table.
	// https://docs.databend.com/sql/sql-reference/file-format-options#empty_field_as
	// default is `string`
	// databend version should >= v1.2.345-nightly
	EmptyFieldAs        string
	EnableOpenTelemetry bool
}

// NewConfig creates a new config with default values
func NewConfig() *Config {
	return &Config{
		Host:     fmt.Sprintf("%s:443", defaultDomain),
		Location: time.UTC,
		Params:   make(map[string]string),
	}
}

// FormatDSN formats the given Config into a DSN string which can be passed to
// the driver.
func (cfg *Config) FormatDSN() string {
	u := &url.URL{
		Host:   cfg.Host,
		Scheme: defaultScheme,
		Path:   "/",
	}
	if len(cfg.User) > 0 {
		if len(cfg.Password) > 0 {
			u.User = url.UserPassword(cfg.User, cfg.Password)
		} else {
			u.User = url.User(cfg.User)
		}
	}
	if len(cfg.Database) > 0 {
		u.Path = cfg.Database
	}
	query := u.Query()
	if cfg.Tenant != "" {
		query.Set("tenant", cfg.Tenant)
	}
	if cfg.Warehouse != "" {
		query.Set("warehouse", cfg.Warehouse)
	}

	if len(cfg.Role) > 0 {
		query.Set("role", cfg.Role)
	}
	if cfg.AccessToken != "" {
		query.Set("access_token", cfg.AccessToken)
	}
	if cfg.AccessTokenFile != "" {
		query.Set("access_token_file", cfg.AccessTokenFile)
	}
	if cfg.Timeout != 0 {
		query.Set("timeout", cfg.Timeout.String())
	}
	if cfg.WaitTimeSecs != 0 {
		query.Set("wait_time_secs", strconv.FormatInt(cfg.WaitTimeSecs, 10))
	}
	if cfg.MaxRowsInBuffer != 0 {
		query.Set("max_rows_in_buffer", strconv.FormatInt(cfg.MaxRowsInBuffer, 10))
	}
	if cfg.MaxRowsPerPage != 0 {
		query.Set("max_rows_per_page", strconv.FormatInt(cfg.MaxRowsPerPage, 10))
	}
	if cfg.Location != time.UTC && cfg.Location != nil {
		query.Set("location", cfg.Location.String())
	}
	if cfg.GzipCompression {
		query.Set("enable_http_compression", "1")
	}
	if cfg.Debug {
		query.Set("debug", "1")
	}
	if cfg.TLSConfig != "" {
		query.Set("tls_config", cfg.TLSConfig)
	}
	if cfg.SSLMode != "" {
		query.Set("sslmode", cfg.SSLMode)
	}
	if cfg.EnableOpenTelemetry {
		query.Set("enable_otel", "true")
	}
	if cfg.PresignedURLDisabled {
		query.Set("presigned_url_disabled", "1")
	}
	if cfg.EmptyFieldAs != "" {
		query.Set("empty_field_as", cfg.EmptyFieldAs)
	} else {
		query.Set("empty_field_as", "string")
	}
	// Add Params to the query
	for k, v := range cfg.Params {
		query.Set(k, v)
	}

	u.RawQuery = query.Encode()
	return u.String()
}

func (cfg *Config) AddParams(params map[string]string) (err error) {
	cfg.makeDefaultConfigValue()
	for k, v := range params {
		switch k {
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(v)
		case "wait_time_secs":
			cfg.WaitTimeSecs, err = strconv.ParseInt(v, 10, 64)
		case "max_rows_in_buffer":
			cfg.MaxRowsInBuffer, err = strconv.ParseInt(v, 10, 64)
		case "max_rows_per_page":
			cfg.MaxRowsPerPage, err = strconv.ParseInt(v, 10, 64)
		case "location":
			cfg.Location, err = time.LoadLocation(v)
		case "debug":
			cfg.Debug, err = strconv.ParseBool(v)
		case "enable_http_compression":
			cfg.GzipCompression, err = strconv.ParseBool(v)
			cfg.Params[k] = v
		case "presigned_url_disabled":
			cfg.PresignedURLDisabled, err = strconv.ParseBool(v)
		case "empty_field_as":
			cfg.EmptyFieldAs = v
		case "tls_config":
			cfg.TLSConfig = v
		case "tenant":
			cfg.Tenant = v
		case "warehouse":
			cfg.Warehouse = v
		case "role":
			cfg.Role = v
		case "access_token":
			cfg.AccessToken = v
		case "access_token_file":
			cfg.AccessTokenFile = v
		case "sslmode":
			cfg.SSLMode = v
		case "enable_otel":
			cfg.EnableOpenTelemetry, err = strconv.ParseBool(v)
		case "default_format", "query", "database":
			return fmt.Errorf("unknown option '%s'", k)
		default:
			cfg.Params[k] = v
		}
	}

	return
}

func (cfg *Config) makeDefaultConfigValue() {
	if cfg.EmptyFieldAs == "" {
		cfg.EmptyFieldAs = "string"
	}
}

func needEscape(s string) bool {
	unescaped, err := url.QueryUnescape(s)
	if err != nil {
		return true
	}
	return url.QueryEscape(unescaped) != s
}

func autoEncodeUserPassInDSN(dsn string) (string, error) {
	i := strings.Index(dsn, "://")
	if i == -1 {
		return dsn, nil
	}
	rest := dsn[i+3:]
	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return dsn, nil
	}
	userinfo := rest[:atIdx]
	user := userinfo
	pass := ""
	if idx := strings.Index(userinfo, ":"); idx != -1 {
		user = userinfo[:idx]
		pass = userinfo[idx+1:]
	}
	var encUser, encPass string
	if needEscape(user) {
		encUser = url.QueryEscape(user)
	} else {
		encUser = user
	}
	if needEscape(pass) {
		encPass = url.QueryEscape(pass)
	} else {
		encPass = pass
	}
	var encUserinfo string
	if pass != "" {
		encUserinfo = encUser + ":" + encPass
	} else {
		encUserinfo = encUser
	}
	encodedDSN := dsn[:i+3] + encUserinfo + rest[atIdx:]
	return encodedDSN, nil
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	encodedDSN, err := autoEncodeUserPassInDSN(dsn)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(encodedDSN)
	if err != nil {
		logger.Error("ParseDSN", "err", err)
		return nil, err
	}
	cfg := NewConfig()

	if strings.HasSuffix(u.Scheme, "http") {
		cfg.SSLMode = SSL_MODE_DISABLE
	}

	if len(u.Path) > 1 {
		cfg.Database = u.Path[1:]
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		if passwd, ok := u.User.Password(); ok {
			cfg.Password = passwd
		}
	}

	params := make(map[string]string)
	for k, v := range u.Query() {
		if len(v) == 0 {
			continue
		}
		params[k] = v[0]
	}

	if err = cfg.AddParams(params); err != nil {
		return nil, err
	}

	if _, _, err := net.SplitHostPort(u.Host); err == nil {
		cfg.Host = u.Host
	} else {
		switch cfg.SSLMode {
		case SSL_MODE_DISABLE:
			cfg.Host = net.JoinHostPort(u.Host, "80")
		default:
			cfg.Host = net.JoinHostPort(u.Host, "443")
		}
	}
	return cfg, nil
}
func (cfg *Config) Connect(ctx context.Context) (driver.Conn, error) {
	return DatabendDriver{}.OpenWithConfig(ctx, cfg)
}

func (cfg *Config) Driver() driver.Driver {
	return DatabendDriver{}
}
