package godatabend

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDriverRegistration(t *testing.T) {
	for _, name := range []string{"databend", "lake"} {
		db, err := sql.Open(name, "databend+https://user:pass@localhost:8000/default")
		assert.NoError(t, err, "sql.Open(%q) should not error", name)
		assert.NotNil(t, db, "sql.Open(%q) should return a non-nil db", name)
		db.Close()
	}
}
