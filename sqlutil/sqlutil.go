package sqlutil

import (
	"database/sql"
	"fmt"
)

// TxStmt wraps an SQL stmt inside an optional transaction.
// If the transaction is nil then it returns the original statement that will
// run outside of a transaction.
// Otherwise returns a copy of the statement that will run inside the transaction.
func TxStmt(transaction *sql.Tx, statement *sql.Stmt) *sql.Stmt {
	if transaction != nil {
		statement = transaction.Stmt(statement)
	}
	return statement
}

// EndTransaction ends a transaction.
// If the transaction succeeded then it is committed, otherwise it is rolledback.
// You MUST check the error returned from this function to be sure that the transaction
// was applied correctly. For example, 'database is locked' errors in sqlite will happen here.
func EndTransaction(txn *sql.Tx, succeeded *bool) error {
	if *succeeded {
		return txn.Commit() // nolint: errcheck
	} else {
		return txn.Rollback() // nolint: errcheck
	}
}

// WithTransaction runs a block of code passing in an SQL transaction
// If the code returns an error or panics then the transactions is rolledback
// Otherwise the transaction is committed.
func WithTransaction(db *sql.DB, fn func(txn *sql.Tx) error) (err error) {
	txn, err := db.Begin()
	if err != nil {
		return fmt.Errorf("sqlutil.WithTransaction.Begin: %w", err)
	}
	succeeded := false
	defer func() {
		err2 := EndTransaction(txn, &succeeded)
		if err == nil && err2 != nil { // failed to commit/rollback
			err = err2
		}
	}()

	err = fn(txn)
	if err != nil {
		return
	}

	succeeded = true
	return
}
