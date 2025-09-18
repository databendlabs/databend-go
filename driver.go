package godatabend

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// DatabendDriver is a context of Go Driver
type DatabendDriver struct{}

// Open creates a new connection.
func (d DatabendDriver) Open(dsn string) (driver.Conn, error) {
	logger.Info("Open")
	ctx := context.Background()
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return d.OpenWithConfig(ctx, cfg)
}

func (d DatabendDriver) OpenConnector(dsn string) (driver.Connector, error) {
	return ParseDSN(dsn)
}

// OpenWithConfig creates a new connection with the given Config.
func (d DatabendDriver) OpenWithConfig(
	ctx context.Context,
	config *Config,
) (driver.Conn, error) {
	logger.Info("OpenWithConfig")
	dc, err := buildDatabendConn(ctx, config)
	if err != nil {
		return nil, err
	}
	return dc, nil
}

var logger = CreateDefaultLogger()

func init() {
	sql.Register("databend", DatabendDriver{})
	_ = logger.SetLogLevel("error")
}
