package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/matrix-org/naffka/sqlutil"
	"github.com/matrix-org/naffka/types"
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
	message_key BYTEA,
	message_value BYTEA NOT NULL,
	message_timestamp_ns BIGINT NOT NULL,
	UNIQUE (topic_nid, message_offset)
);
`

const insertTopicSQL = "" +
	"INSERT INTO naffka_topics (topic_name, topic_nid) VALUES ($1, $2)" +
	" ON CONFLICT DO NOTHING"

const selectNextTopicNIDSQL = "" +
	"SELECT nextval('naffka_topic_nid_seq') AS topic_nid"

const selectTopicSQL = "" +
	"SELECT topic_nid FROM naffka_topics WHERE topic_name = $1"

const selectTopicsSQL = "" +
	"SELECT topic_name, topic_nid FROM naffka_topics"

const insertTopicsSQL = "" +
	"INSERT INTO naffka_messages (topic_nid, message_offset, message_key, message_value, message_timestamp_ns)" +
	" VALUES ($1, $2, $3, $4, $5)"

const selectMessagesSQL = "" +
	"SELECT message_offset, message_key, message_value, message_timestamp_ns" +
	" FROM naffka_messages WHERE topic_nid = $1 AND $2 <= message_offset AND message_offset < $3" +
	" ORDER BY message_offset ASC"

const selectMaxOffsetSQL = "" +
	"SELECT message_offset FROM naffka_messages WHERE topic_nid = $1" +
	" ORDER BY message_offset DESC LIMIT 1"

type topicsStatements struct {
	DB                     *sql.DB
	insertTopicStmt        *sql.Stmt
	insertTopicsStmt       *sql.Stmt
	selectNextTopicNIDStmt *sql.Stmt
	selectTopicStmt        *sql.Stmt
	selectTopicsStmt       *sql.Stmt
	selectMessagesStmt     *sql.Stmt
	selectMaxOffsetStmt    *sql.Stmt
}

func NewPostgresTopicsTable(db *sql.DB) (s *topicsStatements, err error) {
	s = &topicsStatements{
		DB: db,
	}
	_, err = db.Exec(postgresqlSchema)
	if err != nil {
		return
	}

	if s.insertTopicStmt, err = db.Prepare(insertTopicSQL); err != nil {
		return
	}
	if s.selectNextTopicNIDStmt, err = db.Prepare(selectNextTopicNIDSQL); err != nil {
		return
	}
	if s.selectTopicStmt, err = db.Prepare(selectTopicSQL); err != nil {
		return
	}
	if s.selectTopicsStmt, err = db.Prepare(selectTopicsSQL); err != nil {
		return
	}
	if s.insertTopicsStmt, err = db.Prepare(insertTopicsSQL); err != nil {
		return
	}
	if s.selectMessagesStmt, err = db.Prepare(selectMessagesSQL); err != nil {
		return
	}
	if s.selectMaxOffsetStmt, err = db.Prepare(selectMaxOffsetSQL); err != nil {
		return
	}
	return
}

func (t *topicsStatements) InsertTopic(
	ctx context.Context, txn *sql.Tx, topicName string, topicNID int64,
) error {
	stmt := sqlutil.TxStmt(txn, t.insertTopicStmt)
	_, err := stmt.ExecContext(ctx, topicName, topicNID)
	return err
}

func (t *topicsStatements) SelectNextTopicNID(
	ctx context.Context, txn *sql.Tx,
) (topicNID int64, err error) {
	stmt := sqlutil.TxStmt(txn, t.selectNextTopicNIDStmt)
	err = stmt.QueryRowContext(ctx).Scan(&topicNID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return
}

func (t *topicsStatements) SelectTopic(
	ctx context.Context, txn *sql.Tx, topicName string,
) (topicNID int64, err error) {
	stmt := sqlutil.TxStmt(txn, t.selectTopicStmt)
	err = stmt.QueryRowContext(ctx, topicName).Scan(&topicNID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return
}

func (t *topicsStatements) SelectTopics(
	ctx context.Context, txn *sql.Tx,
) (map[string]int64, error) {
	stmt := sqlutil.TxStmt(txn, t.selectTopicsStmt)
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]int64{}
	for rows.Next() {
		var (
			topicName string
			topicNID  int64
		)
		if err = rows.Scan(&topicName, &topicNID); err != nil {
			return nil, err
		}
		result[topicName] = topicNID
	}
	return result, nil
}

func (t *topicsStatements) InsertTopics(
	ctx context.Context, txn *sql.Tx, topicNID int64, messageOffset int64,
	topicKey, topicValue []byte, messageTimestampNs int64,
) error {
	stmt := sqlutil.TxStmt(txn, t.insertTopicsStmt)
	_, err := stmt.ExecContext(ctx, topicNID, messageOffset, topicKey, topicValue, messageTimestampNs)
	return err
}

func (t *topicsStatements) SelectMessages(
	ctx context.Context, txn *sql.Tx, topicNID int64, startOffset, endOffset int64,
) ([]types.Message, error) {
	stmt := sqlutil.TxStmt(txn, t.selectMessagesStmt)
	rows, err := stmt.QueryContext(ctx, topicNID, startOffset, endOffset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []types.Message{}
	for rows.Next() {
		var msg types.Message
		var ts int64
		if err = rows.Scan(&msg.Offset, &msg.Key, &msg.Value, &ts); err != nil {
			return nil, err
		}
		msg.Timestamp = time.Unix(0, ts)
		result = append(result, msg)
	}
	return result, nil
}

func (t *topicsStatements) SelectMaxOffset(
	ctx context.Context, txn *sql.Tx, topicNID int64,
) (offset int64, err error) {
	stmt := sqlutil.TxStmt(txn, t.selectMaxOffsetStmt)
	err = stmt.QueryRowContext(ctx, topicNID).Scan(&offset)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return
}
