package naffka

import (
	"database/sql"
	"sync"
	"time"
)

const postgresqlSchema = `
CREATE SEQUENCE IF NOT EXISTS naffka_topic_nid_seq;
CREATE TABLE IF NOT EXISTS naffka_topics (
	topic_name TEXT PRIMARY KEY,
	topic_nid  BIGINT NOT NULL DEFAULT nextval('naffka_topic_nid_seq')
)

CREATE TABLE IF NOT EXISTS naffka_messages (
	topic_nid BIGINT NOT NULL,
	message_offset BIGINT NOT NULL,
	message_key BYTEA NOT NULL,
	message_value BYTEA NOT NULL,
	message_timestamp_ns BIGINT NOT NULL,
	UNIQUE (topic_nid, message_offset)
);
`

const insertTopicSQL = "" +
	"INSERT INTO naffka_topics (topic_name) VALUES ($1)" +
	" ON CONFLICT DO NOTHING RETURNING (room_nid)"

const selectTopicSQL = "" +
	"SELECT topic_nid FROM naffka_topics WHERE topic_name = $1"

const selectTopicsSQL = "" +
	"SELECT topic_name, topic_nid FROM naffka_topics"

const insertMessageSQL = "" +
	"INSERT INTO naffka_messages (topic_nid, message_offset, message_key, message_value, message_timestamp)" +
	" VALUES ($1, $2, $3, $4, $5)"

const selectMessagesSQL = "" +
	"SELECT message_offset, message_key, message_value, message_timestamp_ns" +
	" FROM naffka_messages WHERE topic_nid = $1 AND $2 < message_offset AND message_offset < $3" +
	" ORDER BY message_offset ASC"

const selectMaxOffsetSQL = "" +
	"SELECT topic_nid, MAX(message_offset) FROM naffka_messages GROUP BY topic_nid"

type postgresqlDatabase struct {
	db                  *sql.DB
	topicsMutex         sync.Mutex
	topicNIDs           map[string]int64
	insertTopicStmt     *sql.Stmt
	selectTopicStmt     *sql.Stmt
	selectTopicsStmt    *sql.Stmt
	insertMessageStmt   *sql.Stmt
	selectMessagesStmt  *sql.Stmt
	selectMaxOffsetStmt *sql.Stmt
}

// NewPostgresqlDatabase creates a new naffka database using a postgresql database.
// Returns an error if there was a problem setting up the database.
func NewPostgresqlDatabase(db *sql.DB) (Database, error) {
	var err error

	p := &postgresqlDatabase{
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
		{insertTopicSQL, &p.insertTopicStmt},
		{selectTopicSQL, &p.selectTopicStmt},
		{selectTopicsSQL, &p.selectTopicsStmt},
		{insertMessageSQL, &p.insertMessageStmt},
		{selectMessagesSQL, &p.selectMessagesStmt},
		{selectMaxOffsetSQL, &p.selectMaxOffsetStmt},
	} {
		*s.stmt, err = db.Prepare(s.sql)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *postgresqlDatabase) StoreMessages(messages []Message) error {
	return withTransaction(p.db, func(txn *sql.Tx) error {
		s := txn.Stmt(p.insertMessageStmt)
		topics := map[string]int64{}
		for _, m := range messages {
			topicNID, err := p.assignTopicNID(txn, topics, m.Topic)
			if err != nil {
				return err
			}
			_, err = s.Exec(topicNID, m.Offset, m.Key, m.Value, m.Timestamp.UnixNano())
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *postgresqlDatabase) FetchMessages(topic string, startOffset, endOffset int64) (messages []Message, err error) {
	topicNID, err := p.getTopicNID(nil, nil, topic)
	if err != nil {
		return
	}
	rows, err := p.selectMaxOffsetStmt.Query(topicNID, startOffset, endOffset)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var (
			offset        int64
			key           []byte
			value         []byte
			timestampNano int64
		)
		if err = rows.Scan(&offset, &key, &value, &timestampNano); err != nil {
			return
		}
		messages = append(messages, Message{
			Topic:     topic,
			Offset:    offset,
			Key:       key,
			Value:     value,
			Timestamp: time.Unix(0, timestampNano),
		})
	}
	return
}

func (p *postgresqlDatabase) MaxOffsets() (map[string]int64, error) {
	maxOffsets, err := p.selectMaxOffsets()
	if err != nil {
		return nil, err
	}
	topicNames, err := p.selectTopics()
	if err != nil {
		return nil, err
	}
	result := map[string]int64{}
	for topicNID, maxOffset := range maxOffsets {
		result[topicNames[topicNID]] = maxOffset
	}
	return result, nil
}

func (p *postgresqlDatabase) selectTopics() (map[int64]string, error) {
	rows, err := p.selectTopicsStmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[int64]string{}
	for rows.Next() {
		var (
			topicName string
			topicNID  int64
		)
		if err = rows.Scan(&topicName, &topicNID); err != nil {
			return nil, err
		}
		result[topicNID] = topicName
	}
	return result, nil
}

func (p *postgresqlDatabase) selectMaxOffsets() (map[int64]int64, error) {
	rows, err := p.selectMaxOffsetStmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[int64]int64{}
	for rows.Next() {
		var (
			topicNID  int64
			maxOffset int64
		)
		if err = rows.Scan(&topicNID, &maxOffset); err != nil {
			return nil, err
		}
		result[topicNID] = maxOffset
	}
	return result, nil
}

func (p *postgresqlDatabase) getTopicNID(txn *sql.Tx, topicNIDs map[string]int64, topicName string) (topicNID int64, err error) {
	// Get from the local cache.
	topicNID = topicNIDs[topicName]
	if topicNID != 0 {
		return topicNID, nil
	}
	// Get from the shared cache.
	p.topicsMutex.Lock()
	topicNID = p.topicNIDs[topicName]
	p.topicsMutex.Unlock()
	if topicNID != 0 {
		if topicNIDs != nil {
			topicNIDs[topicName] = topicNID
		}
		return topicNID, nil
	}
	// Get from the database
	s := p.selectTopicStmt
	if txn != nil {
		s = txn.Stmt(s)
	}
	err = s.QueryRow(topicName).Scan(&topicNID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	// Update the shared cache.
	p.topicsMutex.Lock()
	p.topicNIDs[topicName] = topicNID
	p.topicsMutex.Unlock()
	// Update the local cache.
	if topicNIDs != nil {
		topicNIDs[topicName] = topicNID
	}
	return topicNID, nil
}

func (p *postgresqlDatabase) assignTopicNID(txn *sql.Tx, topicNIDs map[string]int64, topicName string) (topicNID int64, err error) {
	topicNID, err = p.getTopicNID(txn, topicNIDs, topicName)
	if err != nil {
		return 0, err
	}
	if topicNID != 0 {
		return topicNID, err
	}
	err = txn.Stmt(p.insertTopicStmt).QueryRow(topicName).Scan(&topicNID)
	if err == sql.ErrNoRows {
		return p.getTopicNID(txn, topicNIDs, topicName)
	}
	if err != nil {
		return 0, err
	}
	// Update the shared cache.
	p.topicsMutex.Lock()
	p.topicNIDs[topicName] = topicNID
	p.topicsMutex.Unlock()
	// Update the local cache.
	if topicNIDs != nil {
		topicNIDs[topicName] = topicNID
	}
	return topicNID, nil
}

// withTransaction runs a block of code passing in an SQL transaction
// If the code returns an error or panics then the transactions is rolledback
// Otherwise the transaction is committed.
func withTransaction(db *sql.DB, fn func(txn *sql.Tx) error) (err error) {
	txn, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			txn.Rollback()
			panic(r)
		} else if err != nil {
			txn.Rollback()
		} else {
			err = txn.Commit()
		}
	}()
	err = fn(txn)
	return
}
