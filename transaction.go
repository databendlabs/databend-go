package godatabend

import (
	"context"
	"database/sql/driver"
)

type databendTx struct {
	dc *DatabendConn
	// ctx is the BeginTx context, so COMMIT and ROLLBACK carry the same
	// request metadata (such as the user agent) as the BEGIN they end.
	ctx context.Context
}

// txContext derives the context COMMIT and ROLLBACK run on from the BeginTx
// context. database/sql calls Commit and Rollback without a context, and calls
// Rollback precisely when the BeginTx context is canceled, so its cancellation
// must not apply. Its query ID must not either: Databend treats a repeated
// query ID as a retry and returns the first query's result, so a COMMIT
// reusing the BEGIN's ID would report success without committing. Each
// statement gets a fresh ID from checkQueryID instead.
func txContext(ctx context.Context) context.Context {
	return contextWithoutQueryID{Context: context.WithoutCancel(ctx)}
}

func (tx *databendTx) Commit() (err error) {
	if tx.dc == nil || tx.dc.rest == nil {
		return driver.ErrBadConn
	}
	// The transaction died with the server-side session state; nothing of it
	// can be committed. The server would answer COMMIT in Fail state with
	// success without committing anything, so fail here explicitly.
	if err := tx.dc.rest.checkSessionValid(); err != nil {
		return badConnIfInvalidated(err)
	}
	// compatible with old server version
	if tx.dc.rest.sessionState.TxnState != "" {
		_, err = tx.dc.exec(tx.ctx, "COMMIT", nil, nil)
		if err != nil {
			return
		}
	}
	return
}

func (tx *databendTx) Rollback() (err error) {
	if tx.dc == nil || tx.dc.rest == nil {
		return driver.ErrBadConn
	}
	// Nothing left to roll back on the server; a ROLLBACK would only clear
	// the Fail marker of a session that is discarded anyway. Let database/sql
	// release the connection; IsValid then discards it.
	if tx.dc.rest.SessionInvalidated() {
		return nil
	}
	_, err = tx.dc.exec(tx.ctx, "ROLLBACK", nil, nil)
	if err != nil {
		return
	}
	return
}
