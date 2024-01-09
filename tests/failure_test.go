package tests

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionError(t *testing.T) {
	r := require.New(t)

	db, err := sql.Open("databend", "databend://root:123456@localhost:12345")
	r.Nil(err)

	err = db.Ping()
	r.NotNil(err)
}
