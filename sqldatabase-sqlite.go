package naffka

import (
	"database/sql"
)

const sqliteSchema = `
-- The topic table assigns each topic a unique numeric ID.
CREATE TABLE IF NOT EXISTS naffka_topics (
	topic_name TEXT UNIQUE,
	topic_nid  INTEGER PRIMARY KEY AUTOINCREMENT
);

-- The messages table contains the actual messages.
CREATE TABLE IF NOT EXISTS naffka_messages (
	topic_nid INTEGER NOT NULL,
	message_offset BIGINT NOT NULL,
	message_key BYTEA NOT NULL,
	message_value BYTEA NOT NULL,
	message_timestamp_ns BIGINT NOT NULL,
	UNIQUE (topic_nid, message_offset)
);
`

const sqliteInsertTopicSQL = "" +
	"INSERT INTO naffka_topics (topic_name, topic_nid) VALUES ($1, $2)" +
	" ON CONFLICT DO NOTHING"

const sqliteSelectNextTopicNID = "" +
	"SELECT COUNT(topic_nid) FROM naffka_topics"

const sqliteSelectTopicSQL = "" +
	"SELECT topic_nid FROM naffka_topics WHERE topic_name = $1"

const sqliteSelectTopicsSQL = "" +
	"SELECT topic_name, topic_nid FROM naffka_topics"

const sqliteInsertTopicsSQL = "" +
	"INSERT INTO naffka_messages (topic_nid, message_offset, message_key, message_value, message_timestamp_ns)" +
	" VALUES ($1, $2, $3, $4, $5)"

const sqliteSelectMessagesSQL = "" +
	"SELECT message_offset, message_key, message_value, message_timestamp_ns" +
	" FROM naffka_messages WHERE topic_nid = $1 AND $2 <= message_offset AND message_offset < $3" +
	" ORDER BY message_offset ASC"

const sqliteSelectMaxOffsetSQL = "" +
	"SELECT message_offset FROM naffka_messages WHERE topic_nid = $1" +
	" ORDER BY message_offset DESC LIMIT 1"

// NewSqliteDatabase creates a new naffka database using a sqlite database.
// Returns an error if there was a problem setting up the database.
func NewSqliteDatabase(db *sql.DB) (*DatabaseImpl, error) {
	var err error

	p := &DatabaseImpl{
		db:        db,
		topicNIDs: map[string]int64{},
	}

	if _, err = db.Exec(sqliteSchema); err != nil {
		return nil, err
	}

	for _, s := range []struct {
		sql  string
		stmt **sql.Stmt
	}{
		{sqliteInsertTopicSQL, &p.insertTopicStmt},
		{sqliteSelectNextTopicNID, &p.selectNextTopicNIDStmt},
		{sqliteSelectTopicSQL, &p.selectTopicStmt},
		{sqliteSelectTopicsSQL, &p.selectTopicsStmt},
		{sqliteInsertTopicsSQL, &p.insertMessageStmt},
		{sqliteSelectMessagesSQL, &p.selectMessagesStmt},
		{sqliteSelectMaxOffsetSQL, &p.selectMaxOffsetStmt},
	} {
		*s.stmt, err = db.Prepare(s.sql)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
