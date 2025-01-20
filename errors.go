package godatabend

import (
	"github.com/pkg/errors"
)

var (
	ErrPlaceholderCount = errors.New("databend: wrong placeholder count")
	ErrNoLastInsertID   = errors.New("no LastInsertId available")
	ErrNoRowsAffected   = errors.New("no RowsAffected available")
)
