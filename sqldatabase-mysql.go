package naffka

import (
	"database/sql"
)

const mysqlSchema = `
-- The topic table assigns each topic a unique numeric ID.
CREATE TABLE IF NOT EXISTS naffka_topics (
	topic_name LONGTEXT UNIQUE,
	topic_nid  BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY
);

-- The messages table contains the actual messages.
CREATE TABLE IF NOT EXISTS naffka_messages (
	topic_nid BIGINT NOT NULL,
	message_offset BIGINT NOT NULL,
	message_key TEXT NOT NULL,
	message_value TEXT NOT NULL,
	message_timestamp_ns BIGINT NOT NULL,
	UNIQUE (topic_nid, message_offset)
);
`

const mysqlInsertTopicSQL = "" +
	"INSERT INTO naffka_topics (topic_name, topic_nid) VALUES ($1, $2)" +
	" ON CONFLICT DO NOTHING"

const mysqlSelectNextTopicNIDSQL = "" +
	"SELECT COUNT(topic_nid)+1 AS topic_nid FROM naffka_topics"

const mysqlSelectTopicSQL = "" +
	"SELECT topic_nid FROM naffka_topics WHERE topic_name = $1"

const mysqlSelectTopicsSQL = "" +
	"SELECT topic_name, topic_nid FROM naffka_topics"

const mysqlInsertTopicsSQL = "" +
	"INSERT INTO naffka_messages (topic_nid, message_offset, message_key, message_value, message_timestamp_ns)" +
	" VALUES ($1, $2, $3, $4, $5)"

const mysqlISelectMessagesSQL = "" +
	"SELECT message_offset, message_key, message_value, message_timestamp_ns" +
	" FROM naffka_messages WHERE topic_nid = $1 AND $2 <= message_offset AND message_offset < $3" +
	" ORDER BY message_offset ASC"

const mysqlSelectMaxOffsetSQL = "" +
	"SELECT message_offset FROM naffka_messages WHERE topic_nid = $1" +
	" ORDER BY message_offset DESC LIMIT 1"

// NewMysqlDatabase creates a new naffka database using a mysql database.
// Returns an error if there was a problem setting up the database.
func NewMysqlDatabase(db *sql.DB) (*DatabaseImpl, error) {
	var err error

	m := &DatabaseImpl{
		db:        db,
		topicNIDs: map[string]int64{},
	}

	if _, err = db.Exec(mysqlSchema); err != nil {
		return nil, err
	}

	for _, s := range []struct {
		sql  string
		stmt **sql.Stmt
	}{
		{mysqlInsertTopicSQL, &m.insertTopicStmt},
		{mysqlSelectNextTopicNIDSQL, &m.selectNextTopicNIDStmt},
		{mysqlSelectTopicSQL, &m.selectTopicStmt},
		{mysqlSelectTopicsSQL, &m.selectTopicsStmt},
		{mysqlInsertTopicsSQL, &m.insertMessageStmt},
		{mysqlISelectMessagesSQL, &m.selectMessagesStmt},
		{mysqlSelectMaxOffsetSQL, &m.selectMaxOffsetStmt},
	} {
		*s.stmt, err = db.Prepare(s.sql)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
