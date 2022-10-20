package godatabend

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	ErrPlaceholderCount = errors.New("databend: wrong placeholder count")
	ErrNoLastInsertID   = errors.New("no LastInsertId available")
	ErrNoRowsAffected   = errors.New("no RowsAffected available")
)

// Error contains parsed information about server error
type Error struct {
	Code    int
	Message string
}

// Error implements the interface error
func (e *Error) Error() string {
	return fmt.Sprintf("Code: %d, Message: %s", e.Code, e.Message)
}
