package postgres

import (
	"database/sql"

	"github.com/matrix-org/naffka/sqlutil"
	"github.com/matrix-org/naffka/storage/shared"
)

// Database stores information needed by the federation sender
type Database struct {
	shared.Database
	db     *sql.DB
	writer sqlutil.Writer
}

// NewDatabase opens a new database
func NewDatabase(dsn string) (*Database, error) {
	var d Database
	var err error
	if d.db, err = sql.Open("postgres", dsn); err != nil {
		return nil, err
	}
	d.writer = sqlutil.NewDummyWriter()
	topics, err := NewPostgresTopicsTable(d.db)
	if err != nil {
		return nil, err
	}
	d.Database = shared.Database{
		DB:          d.db,
		Writer:      d.writer,
		TopicsTable: topics,
	}
	return &d, nil
}
