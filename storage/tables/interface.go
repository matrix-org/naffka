package tables

import (
	"context"
	"database/sql"

	"github.com/matrix-org/naffka/types"
)

type NaffkaTopics interface {
	InsertTopic(ctx context.Context, txn *sql.Tx, topicName string, topicNID int64) error
	SelectNextTopicNID(ctx context.Context, txn *sql.Tx) (topicNID int64, err error)
	SelectTopic(ctx context.Context, txn *sql.Tx, topicName string) (topicNID int64, err error)
	SelectTopics(ctx context.Context, txn *sql.Tx) (map[string]int64, error)
	InsertTopics(ctx context.Context, txn *sql.Tx, topicNID int64, messageOffset int64, topicKey, topicValue []byte, messageTimestampNs int64) error
	SelectMessages(ctx context.Context, txn *sql.Tx, topicNID, startOffset, endOffset int64) ([]types.Message, error)
	SelectMaxOffset(ctx context.Context, txn *sql.Tx, topicNID int64) (offset int64, err error)
}
