package godatabend

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"
)

const (
	defaultClientTimeout  = 900 * time.Second // Timeout for network round trip + read out http response
	defaultLoginTimeout   = 60 * time.Second  // Timeout for retry for login EXCLUDING clientTimeout
	defaultRequestTimeout = 0 * time.Second   // Timeout for retry for request EXCLUDING clientTimeout
	defaultDomain         = "app.databend.com"
)
const (
	clientType = "Go"
)

// Config is a set of configuration parameters
type Config struct {
	Tenant    string // Tenant
	Warehouse string // Warehouse
	User      string // Username
	Password  string // Password (requires User)
	Database  string // Database name

	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`

	Scheme          string
	Host            string
	Timeout         time.Duration
	IdleTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	Location        *time.Location
	Debug           bool
	UseDBLocation   bool
	GzipCompression bool
	Params          map[string]string
	TLSConfig       string

	PresignedURLDisabled bool
}

// NewConfig creates a new config with default values
func NewConfig() *Config {
	return &Config{
		Scheme:      "https",
		Host:        fmt.Sprintf("%s:443", defaultDomain),
		IdleTimeout: time.Hour,
		Location:    time.UTC,
		Params:      make(map[string]string),
	}
}

// FormatDSN formats the given Config into a DSN string which can be passed to
// the driver.
func (cfg *Config) FormatDSN() string {
	u := cfg.url(nil, true)
	query := u.Query()
	if cfg.Tenant != "" {
		query.Set("tenant", cfg.Tenant)
	}
	if cfg.Warehouse != "" {
		query.Set("warehouse", cfg.Warehouse)
	}
	if cfg.Timeout != 0 {
		query.Set("timeout", cfg.Timeout.String())
	}
	if cfg.IdleTimeout != 0 {
		query.Set("idle_timeout", cfg.IdleTimeout.String())
	}
	if cfg.ReadTimeout != 0 {
		query.Set("read_timeout", cfg.ReadTimeout.String())
	}
	if cfg.WriteTimeout != 0 {
		query.Set("write_timeout", cfg.WriteTimeout.String())
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
	if cfg.PresignedURLDisabled {
		query.Set("presigned_url_disabled", "1")
	}

	u.RawQuery = query.Encode()
	return u.String()
}

func (cfg *Config) url(extra map[string]string, dsn bool) *url.URL {
	u := &url.URL{
		Host:   cfg.Host,
		Scheme: cfg.Scheme,
		Path:   "/",
	}
	if len(cfg.User) > 0 {
		if len(cfg.Password) > 0 {
			u.User = url.UserPassword(cfg.User, cfg.Password)
		} else {
			u.User = url.User(cfg.User)
		}
	}
	query := u.Query()
	if len(cfg.Database) > 0 {
		if dsn {
			u.Path += cfg.Database
		} else {
			query.Set("database", cfg.Database)
		}
	}
	for k, v := range cfg.Params {
		query.Set(k, v)
	}
	for k, v := range extra {
		query.Set(k, v)
	}

	u.RawQuery = query.Encode()
	return u
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	cfg := NewConfig()

	switch u.Scheme {
	case "http", "https":
		cfg.Scheme = u.Scheme
	case "db+http", "db+https":
		cfg.Scheme = u.Scheme[len("db+"):]
	case "databend+http", "databend+https":
		cfg.Scheme = u.Scheme[len("databend+"):]
	case "databend", "db":
		if u.Query().Get("sslmode") == "disable" {
			cfg.Scheme = "http"
		} else {
			cfg.Scheme = "https"
		}
	default:
		return nil, fmt.Errorf("invalid scheme: %s", cfg.Scheme)
	}

	if _, _, err := net.SplitHostPort(u.Host); err == nil {
		cfg.Host = u.Host
	} else {
		switch cfg.Scheme {
		case "http":
			cfg.Host = net.JoinHostPort(u.Host, "80")
		case "https":
			cfg.Host = net.JoinHostPort(u.Host, "443")
		default:
			return nil, fmt.Errorf("invalid scheme: %s", cfg.Scheme)
		}
	}

	if len(u.Path) > 1 {
		// skip '/'
		cfg.Database = u.Path[1:]
	}
	if u.User != nil {
		// it is expected that empty password will be dropped out on Parse and Format
		cfg.User = u.User.Username()
		if passwd, ok := u.User.Password(); ok {
			cfg.Password = passwd
		}
	}
	if err = parseDSNParams(cfg, map[string][]string(u.Query())); err != nil {
		return nil, err
	}
	return cfg, nil
}

// parseDSNParams parses the DSN "query string"
// Values must be url.QueryEscape'ed
func parseDSNParams(cfg *Config, params map[string][]string) (err error) {
	for k, v := range params {
		if len(v) == 0 {
			continue
		}

		switch k {
		case "timeout":
			cfg.Timeout, err = time.ParseDuration(v[0])
		case "idle_timeout":
			cfg.IdleTimeout, err = time.ParseDuration(v[0])
		case "read_timeout":
			cfg.ReadTimeout, err = time.ParseDuration(v[0])
		case "write_timeout":
			cfg.WriteTimeout, err = time.ParseDuration(v[0])
		case "location":
			cfg.Location, err = time.LoadLocation(v[0])
		case "debug":
			cfg.Debug, err = strconv.ParseBool(v[0])
		case "default_format", "query", "database":
			err = fmt.Errorf("unknown option '%s'", k)
		case "enable_http_compression":
			cfg.GzipCompression, err = strconv.ParseBool(v[0])
			cfg.Params[k] = v[0]
		case "presigned_url_disabled":
			cfg.PresignedURLDisabled, err = strconv.ParseBool(v[0])
			cfg.Params[k] = v[0]
		case "tls_config":
			cfg.TLSConfig = v[0]
		case "tenant":
			cfg.Tenant = v[0]
		case "warehouse":
			cfg.Warehouse = v[0]
		case "access_token":
			cfg.AccessToken = v[0]
		case "refresh_token":
			cfg.RefreshToken = v[0]
		case "sslmode":
			// ignore
		default:
			cfg.Params[k] = v[0]
		}
		if err != nil {
			return err
		}
	}

	return
}
