package naffka

import (
	"database/sql"
	"sync"
	"time"
)

type DatabaseImpl struct {
	db                     *sql.DB
	topicsMutex            sync.Mutex
	topicNIDs              map[string]int64
	insertTopicStmt        *sql.Stmt
	selectNextTopicNIDStmt *sql.Stmt
	selectTopicStmt        *sql.Stmt
	selectTopicsStmt       *sql.Stmt
	insertMessageStmt      *sql.Stmt
	selectMessagesStmt     *sql.Stmt
	selectMaxOffsetStmt    *sql.Stmt
}

// StoreMessages implements Database.
func (p *DatabaseImpl) StoreMessages(topic string, messages []Message) error {
	// Store the messages inside a single database transaction.
	return withTransaction(p.db, func(txn *sql.Tx) error {
		s := txn.Stmt(p.insertMessageStmt)
		topicNID, err := p.assignTopicNID(txn, topic)
		if err != nil {
			return err
		}
		for _, m := range messages {
			_, err = s.Exec(topicNID, m.Offset, m.Key, m.Value, m.Timestamp.UnixNano())
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// FetchMessages implements Database.
func (p *DatabaseImpl) FetchMessages(topic string, startOffset, endOffset int64) (messages []Message, err error) {
	topicNID, err := p.getTopicNID(nil, topic)
	if err != nil {
		return
	}
	rows, err := p.selectMessagesStmt.Query(topicNID, startOffset, endOffset)
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
			Offset:    offset,
			Key:       key,
			Value:     value,
			Timestamp: time.Unix(0, timestampNano),
		})
	}
	return
}

// MaxOffsets implements Database.
func (p *DatabaseImpl) MaxOffsets() (map[string]int64, error) {
	topicNames, err := p.selectTopics()
	if err != nil {
		return nil, err
	}
	result := map[string]int64{}
	for topicName, topicNID := range topicNames {
		// Lookup the maximum offset.
		maxOffset, err := p.selectMaxOffset(topicNID)
		if err != nil {
			return nil, err
		}
		if maxOffset > -1 {
			// Don't include the topic if we haven't sent any messages on it.
			result[topicName] = maxOffset
		}
		// Prefill the numeric ID cache.
		p.addTopicNIDToCache(topicName, topicNID)
	}
	return result, nil
}

// selectTopics fetches the names and numeric IDs for all the topics the
// database is aware of.
func (p *DatabaseImpl) selectTopics() (map[string]int64, error) {
	rows, err := p.selectTopicsStmt.Query()
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

// selectMaxOffset selects the maximum offset for a topic.
// Returns -1 if there aren't any messages for that topic.
// Returns an error if there was a problem talking to the database.
func (p *DatabaseImpl) selectMaxOffset(topicNID int64) (maxOffset int64, err error) {
	err = p.selectMaxOffsetStmt.QueryRow(topicNID).Scan(&maxOffset)
	if err == sql.ErrNoRows {
		return -1, nil
	}
	return maxOffset, err
}

// getTopicNID finds the numeric ID for a topic.
// The txn argument is optional, this can be used outside a transaction
// by setting the txn argument to nil.
func (p *DatabaseImpl) getTopicNID(txn *sql.Tx, topicName string) (topicNID int64, err error) {
	// Get from the cache.
	topicNID = p.getTopicNIDFromCache(topicName)
	if topicNID != 0 {
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
	p.addTopicNIDToCache(topicName, topicNID)
	return topicNID, nil
}

// getNextTopicNID finds the numeric ID for the next topic.
// The txn argument is optional, this can be used outside a transaction
// by setting the txn argument to nil.
func (p *DatabaseImpl) getNextTopicNID(txn *sql.Tx, topicName string) (topicNID int64, err error) {
	// Get from the database
	s := p.selectNextTopicNIDStmt
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
	return topicNID, nil
}

// assignTopicNID assigns a new numeric ID to a topic.
// The txn argument is mandatory, this is always called inside a transaction.
func (p *DatabaseImpl) assignTopicNID(txn *sql.Tx, topicName string) (topicNID int64, err error) {
	// Check if we already have a numeric ID for the topic name.
	topicNID, err = p.getTopicNID(txn, topicName)
	if err != nil {
		return 0, err
	}
	if topicNID != 0 {
		return topicNID, nil
	}
	// Get the next topic ID from the database
	err = txn.Stmt(p.selectNextTopicNIDStmt).QueryRow().Scan(&topicNID)
	if err != nil {
		return 0, err
	}
	// We don't have a numeric ID for the topic name so we add an entry to the
	// topics table. If the insert stmt succeeds then it will return the ID.
	_, err = txn.Stmt(p.insertTopicStmt).Exec(topicName, topicNID)
	if err != nil {
		return 0, err
	}
	// Update the cache.
	p.addTopicNIDToCache(topicName, topicNID)
	return topicNID, nil
}

// getTopicNIDFromCache returns the topicNID from the cache or returns 0 if the
// topic is not in the cache.
func (p *DatabaseImpl) getTopicNIDFromCache(topicName string) (topicNID int64) {
	p.topicsMutex.Lock()
	defer p.topicsMutex.Unlock()
	return p.topicNIDs[topicName]
}

// addTopicNIDToCache adds the numeric ID for the topic to the cache.
func (p *DatabaseImpl) addTopicNIDToCache(topicName string, topicNID int64) {
	p.topicsMutex.Lock()
	defer p.topicsMutex.Unlock()
	p.topicNIDs[topicName] = topicNID
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
