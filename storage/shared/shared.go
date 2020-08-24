package shared

import (
	"context"
	"database/sql"
	"sync"

	"github.com/matrix-org/naffka/sqlutil"
	"github.com/matrix-org/naffka/storage/tables"
	"github.com/matrix-org/naffka/types"
)

// Database represents a database implementation, either Postgres
// or SQLite.
type Database struct {
	DB          *sql.DB
	Writer      sqlutil.Writer
	TopicsTable tables.NaffkaTopics
	topicsMutex sync.RWMutex     // cache
	topicNIDs   map[string]int64 // cache
}

// StoreMessages implements Database.
func (p *Database) StoreMessages(topic string, messages []types.Message) error {
	topicNID, err := p.assignTopicNID(topic)
	if err != nil {
		return err
	}
	// Store the messages inside a single database transaction.
	return p.Writer.Do(p.DB, nil, func(txn *sql.Tx) error {
		for _, m := range messages {
			if err := p.TopicsTable.InsertTopics(
				context.TODO(),
				txn,
				topicNID,
				m.Offset,
				m.Key,
				m.Value,
				m.Timestamp.UnixNano(),
			); err != nil {
				return err
			}
		}
		return nil
	})
}

// FetchMessages implements Database.
func (p *Database) FetchMessages(topic string, startOffset, endOffset int64) (messages []types.Message, err error) {
	topicNID, err := p.getTopicNID(topic)
	if err != nil {
		return
	}
	return p.TopicsTable.SelectMessages(context.TODO(), nil, topicNID, startOffset, endOffset)
}

// MaxOffsets implements Database.
func (p *Database) MaxOffsets() (map[string]int64, error) {
	topicNames, err := p.TopicsTable.SelectTopics(context.TODO(), nil)
	if err != nil {
		return nil, err
	}
	result := map[string]int64{}
	for topicName, topicNID := range topicNames {
		// Lookup the maximum offset.
		maxOffset, err := p.TopicsTable.SelectMaxOffset(context.TODO(), nil, topicNID)
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

// getTopicNID finds the numeric ID for a topic.
// The txn argument is optional, this can be used outside a transaction
// by setting the txn argument to nil.
func (p *Database) getTopicNID(topicName string) (topicNID int64, err error) {
	// Get from the cache.
	topicNID = p.getTopicNIDFromCache(topicName)
	if topicNID != 0 {
		return topicNID, nil
	}
	// Get from the database
	topicNID, err = p.TopicsTable.SelectTopic(context.TODO(), nil, topicName)
	if err != nil {
		return 0, err
	}
	// Update the shared cache.
	p.addTopicNIDToCache(topicName, topicNID)
	return topicNID, nil
}

// assignTopicNID assigns a new numeric ID to a topic.
// The txn argument is mandatory, this is always called inside a transaction.
func (p *Database) assignTopicNID(topicName string) (topicNID int64, err error) {
	// Check if we already have a numeric ID for the topic name.
	topicNID, err = p.getTopicNID(topicName)
	if err != nil {
		return 0, err
	}
	if topicNID != 0 {
		return topicNID, nil
	}
	// Assign a new topic NID if we don't.
	err = p.Writer.Do(p.DB, nil, func(txn *sql.Tx) error {
		topicNID, err = p.TopicsTable.SelectNextTopicNID(context.TODO(), txn)
		if err != nil {
			return err
		}
		return p.TopicsTable.InsertTopic(context.TODO(), txn, topicName, topicNID)
	})
	if err != nil {
		return 0, err
	}
	// Update the cache.
	p.addTopicNIDToCache(topicName, topicNID)
	return topicNID, nil
}

func (p *Database) CreateCache() {
	p.topicsMutex.Lock()
	defer p.topicsMutex.Unlock()
	p.topicNIDs = make(map[string]int64)
}

// getTopicNIDFromCache returns the topicNID from the cache or returns 0 if the
// topic is not in the cache.
func (p *Database) getTopicNIDFromCache(topicName string) (topicNID int64) {
	p.topicsMutex.Lock()
	defer p.topicsMutex.Unlock()
	return p.topicNIDs[topicName]
}

// addTopicNIDToCache adds the numeric ID for the topic to the cache.
func (p *Database) addTopicNIDToCache(topicName string, topicNID int64) {
	p.topicsMutex.Lock()
	defer p.topicsMutex.Unlock()
	p.topicNIDs[topicName] = topicNID
}
