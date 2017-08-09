package naffka

import (
	"fmt"
	"log"
	"sync"
	"time"

	sarama "gopkg.in/Shopify/sarama.v1"
)

// Naffka is an implementation of the sarama kafka API designed to run within a
// single go process. It implements both the sarama.SyncProducer and the
// sarama.Consumer interfaces. This means it can act as a drop in replacement
// for kafka for testing or single instance deployment.
type Naffka struct {
	db          Database
	topicsMutex sync.Mutex
	topics      map[string]*topic
}

// New creates a new Naffka instance.
func New(db Database) (*Naffka, error) {
	n := &Naffka{db: db, topics: map[string]*topic{}}
	maxOffsets, err := db.MaxOffsets()
	if err != nil {
		return nil, err
	}
	for topicName, offset := range maxOffsets {
		n.topics[topicName] = &topic{
			topicName:  topicName,
			nextOffset: offset + 1,
		}
	}
	return n, nil
}

// A Message is used internally within naffka to store messages.
// It is converted to a sarama.ConsumerMessage when exposed to the
// public APIs to maintain API compatibility with sarama.
type Message struct {
	Topic     string
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

func (m *Message) consumerMessage() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Topic:     m.Topic,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
	}
}

// A Database is used to store naffka messages.
// Messages are stored so that new consumers can access the full message history.
type Database interface {
	// StoreMessages stores a list of messages.
	StoreMessages(messages []Message) error
	// FetchMessages fetches all messages with an offset greater than startOffset
	// and less than endOffset.
	FetchMessages(topic string, startOffset, endOffset int64) ([]Message, error)
	// MaxOffsets returns the maximum offset for each topic.
	MaxOffsets() (map[string]int64, error)
}

// SendMessage implements sarama.SyncProducer
func (n *Naffka) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	err = n.SendMessages([]*sarama.ProducerMessage{msg})
	return msg.Partition, msg.Offset, err
}

// SendMessages implements sarama.SyncProducer
func (n *Naffka) SendMessages(msgs []*sarama.ProducerMessage) error {
	byTopic := map[string][]*sarama.ProducerMessage{}
	for _, msg := range msgs {
		byTopic[msg.Topic] = append(byTopic[msg.Topic], msg)
	}
	var topicNames []string
	for topicName := range byTopic {
		topicNames = append(topicNames, topicName)
	}

	now := time.Now()
	topics := n.getTopics(topicNames)
	for topicName := range byTopic {
		if err := topics[topicName].send(now, byTopic[topicName]); err != nil {
			return err
		}
	}
	return nil
}

func (n *Naffka) getTopics(topicNames []string) map[string]*topic {
	n.topicsMutex.Lock()
	defer n.topicsMutex.Unlock()
	result := map[string]*topic{}
	for _, topicName := range topicNames {
		t := n.topics[topicName]
		if t == nil {
			// If the topic doesn't already exist then create it.
			t = &topic{db: n.db, topicName: topicName}
			n.topics[topicName] = t
		}
		result[topicName] = t
	}
	return result
}

// Topics implements sarama.Consumer
func (n *Naffka) Topics() ([]string, error) {
	n.topicsMutex.Lock()
	defer n.topicsMutex.Unlock()
	var result []string
	for topic := range n.topics {
		result = append(result, topic)
	}
	return result, nil
}

// Partitions implements sarama.Consumer
func (n *Naffka) Partitions(topic string) ([]int32, error) {
	// Naffka stores a single partition per topic, so this always returns a single partition ID.
	return partitions[:], nil
}

var partitions = [1]int32{0}

// ConsumePartition implements sarama.Consumer
func (n *Naffka) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if partition != 0 {
		return nil, fmt.Errorf("Unknown partition ID %d", partition)
	}
	topics := n.getTopics([]string{topic})
	return topics[topic].consume(offset), nil
}

// HighWaterMarks implements sarama.Consumer
func (n *Naffka) HighWaterMarks() map[string]map[int32]int64 {
	n.topicsMutex.Lock()
	defer n.topicsMutex.Unlock()
	result := map[string]map[int32]int64{}
	for topicName, topic := range n.topics {
		result[topicName] = map[int32]int64{
			0: topic.highwaterMark(),
		}
	}
	return result
}

// Close implements sarama.SyncProducer and sarama.Consumer
func (n *Naffka) Close() error {
	return nil
}

const blockSize = 1024

type partitionConsumer struct {
	topic    *topic
	messages chan *sarama.ConsumerMessage
	ready    bool
}

// AsyncClose implements sarama.PartitionConsumer
func (c *partitionConsumer) AsyncClose() {
}

// Close implements sarama.PartitionConsumer
func (c *partitionConsumer) Close() error {
	return nil
}

// Messages implements sarama.PartitionConsumer
func (c *partitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

// Errors implements sarama.PartitionConsumer
func (c *partitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return nil
}

// HighWaterMarkOffset implements sarama.PartitionConsumer
func (c *partitionConsumer) HighWaterMarkOffset() int64 {
	return c.topic.highwaterMark()
}

func (c *partitionConsumer) block(cmsg *sarama.ConsumerMessage) {
	c.messages <- cmsg
	c.catchup(cmsg.Offset)
}

func (c *partitionConsumer) catchup(fromOffset int64) {
	for {
		caughtUp, nextOffset := c.topic.hasCaughtUp(c, fromOffset)
		if caughtUp {
			return
		}
		if nextOffset > fromOffset+blockSize {
			nextOffset = fromOffset + blockSize
		}

		msgs, err := c.topic.db.FetchMessages(c.topic.topicName, fromOffset, nextOffset)
		if err != nil {
			// TODO: Error handling.
			log.Print("Error: reading messages", err)
		}
		for i := range msgs {
			c.messages <- msgs[i].consumerMessage()
		}
		fromOffset = msgs[len(msgs)-1].Offset
	}
}

type topic struct {
	db         Database
	topicName  string
	mutex      sync.Mutex
	consumers  []*partitionConsumer
	nextOffset int64
}

func (t *topic) send(now time.Time, pmsgs []*sarama.ProducerMessage) error {
	var err error
	msgs := make([]Message, len(pmsgs))
	for i := range msgs {
		msgs[i].Topic = pmsgs[i].Topic
		if pmsgs[i].Key != nil {
			msgs[i].Key, err = pmsgs[i].Key.Encode()
			if err != nil {
				return err
			}
		}
		if pmsgs[i].Value != nil {
			msgs[i].Value, err = pmsgs[i].Value.Encode()
			if err != nil {
				return err
			}
		}
		pmsgs[i].Timestamp = now
		msgs[i].Timestamp = now
	}
	t.mutex.Lock()
	defer t.mutex.Unlock()
	offset := t.nextOffset
	for i := range msgs {
		pmsgs[i].Offset = offset
		msgs[i].Offset = offset
		offset++
	}
	err = t.db.StoreMessages(msgs)
	if err != nil {
		return err
	}
	t.nextOffset = offset

	for i := range msgs {
		cmsg := msgs[i].consumerMessage()
		for _, c := range t.consumers {
			if c.ready {
				select {
				case c.messages <- cmsg:
				default:
					// The consumer wasn't ready to receive a message because
					// the channel buffer was full.
					// Fork a goroutine to send the message so that we don't
					// block sending messages to the other consumers.
					c.ready = false
					go c.block(cmsg)
				}
			}
		}
	}

	return nil
}

func (t *topic) consume(offset int64) *partitionConsumer {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	c := &partitionConsumer{
		topic: t,
	}
	if offset == sarama.OffsetNewest {
		offset = t.nextOffset
	}
	if offset == sarama.OffsetOldest {
		offset = -1
	}
	c.messages = make(chan *sarama.ConsumerMessage, blockSize)
	t.consumers = append(t.consumers, c)
	go c.catchup(offset)
	return c
}

func (t *topic) hasCaughtUp(c *partitionConsumer, offset int64) (bool, int64) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if offset+1 == t.nextOffset {
		c.ready = true
		return true, t.nextOffset
	}
	return false, t.nextOffset
}

func (t *topic) highwaterMark() int64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.nextOffset
}
