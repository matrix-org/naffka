package naffka

import (
	"database/sql"
)

const postgresqlSchema = `
-- The topic table assigns each topic a unique numeric ID.
CREATE SEQUENCE IF NOT EXISTS naffka_topic_nid_seq;
CREATE TABLE IF NOT EXISTS naffka_topics (
	topic_name TEXT PRIMARY KEY,
	topic_nid  BIGINT NOT NULL DEFAULT nextval('naffka_topic_nid_seq')
);

-- The messages table contains the actual messages.
CREATE TABLE IF NOT EXISTS naffka_messages (
	topic_nid BIGINT NOT NULL,
	message_offset BIGINT NOT NULL,
	message_key BYTEA NOT NULL,
	message_value BYTEA NOT NULL,
	message_timestamp_ns BIGINT NOT NULL,
	UNIQUE (topic_nid, message_offset)
);
`

const postgresqlInsertTopicSQL = "" +
	"INSERT INTO naffka_topics (topic_name, topic_nid) VALUES ($1, $2)" +
	" ON CONFLICT DO NOTHING"

const postgresqlSelectNextTopicNID = "" +
	"SELECT nextval('naffka_topic_nid_seq') AS topic_nid"

const postgresqlSelectTopicSQL = "" +
	"SELECT topic_nid FROM naffka_topics WHERE topic_name = $1"

const postgresqlSelectTopicsSQL = "" +
	"SELECT topic_name, topic_nid FROM naffka_topics"

const postgresqlInsertTopicsSQL = "" +
	"INSERT INTO naffka_messages (topic_nid, message_offset, message_key, message_value, message_timestamp_ns)" +
	" VALUES ($1, $2, $3, $4, $5)"

const postgresqlISelectMessagesSQL = "" +
	"SELECT message_offset, message_key, message_value, message_timestamp_ns" +
	" FROM naffka_messages WHERE topic_nid = $1 AND $2 <= message_offset AND message_offset < $3" +
	" ORDER BY message_offset ASC"

const postgresqlSelectMaxOffsetSQL = "" +
	"SELECT message_offset FROM naffka_messages WHERE topic_nid = $1" +
	" ORDER BY message_offset DESC LIMIT 1"

// NewPostgresqlDatabase creates a new naffka database using a postgresql database.
// Returns an error if there was a problem setting up the database.
func NewPostgresqlDatabase(db *sql.DB) (*DatabaseImpl, error) {
	var err error

	p := &DatabaseImpl{
		db:        db,
		topicNIDs: map[string]int64{},
	}

	if _, err = db.Exec(postgresqlSchema); err != nil {
		return nil, err
	}

	for _, s := range []struct {
		sql  string
		stmt **sql.Stmt
	}{
		{postgresqlInsertTopicSQL, &p.insertTopicStmt},
		{postgresqlSelectNextTopicNID, &p.selectNextTopicNIDStmt},
		{postgresqlSelectTopicSQL, &p.selectTopicStmt},
		{postgresqlSelectTopicsSQL, &p.selectTopicsStmt},
		{postgresqlInsertTopicsSQL, &p.insertMessageStmt},
		{postgresqlISelectMessagesSQL, &p.selectMessagesStmt},
		{postgresqlSelectMaxOffsetSQL, &p.selectMaxOffsetStmt},
	} {
		*s.stmt, err = db.Prepare(s.sql)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
