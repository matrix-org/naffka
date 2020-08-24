package naffka

import (
	"fmt"
	"log"
	"sync"
	"time"

	sarama "github.com/Shopify/sarama"
	"github.com/matrix-org/naffka/storage"
	"github.com/matrix-org/naffka/types"
)

// Naffka is an implementation of the sarama kafka API designed to run within a
// single go process. It implements both the sarama.SyncProducer and the
// sarama.Consumer interfaces. This means it can act as a drop in replacement
// for kafka for testing or single instance deployment.
// Does not support multiple partitions.
type Naffka struct {
	db          storage.Database
	topicsMutex sync.Mutex
	topics      map[string]*topic
}

// New creates a new Naffka instance.
func New(db storage.Database) (*Naffka, error) {
	n := &Naffka{db: db, topics: map[string]*topic{}}
	maxOffsets, err := db.MaxOffsets()
	if err != nil {
		return nil, err
	}
	for topicName, offset := range maxOffsets {
		n.topics[topicName] = &topic{
			db:         db,
			topicName:  topicName,
			nextOffset: offset + 1,
		}
	}
	return n, nil
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
	return []int32{0}, nil
}

// ConsumePartition implements sarama.Consumer
// Note: offset is *inclusive*, i.e. it will include the message with that offset.
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

const channelSize = 1024

// partitionConsumer ensures that all messages written to a particular
// topic, from an offset, get sent in order to a channel.
// Implements sarama.PartitionConsumer
type partitionConsumer struct {
	topic    *topic
	messages chan *sarama.ConsumerMessage
	// Whether the consumer is in "catchup" mode or not.
	// See "catchup" function for details.
	// Reads and writes to this field are proctected by the topic mutex.
	catchingUp bool
}

// AsyncClose implements sarama.PartitionConsumer
func (c *partitionConsumer) AsyncClose() {
}

// Close implements sarama.PartitionConsumer
func (c *partitionConsumer) Close() error {
	// TODO: Add support for performing a clean shutdown of the consumer.
	return nil
}

// Messages implements sarama.PartitionConsumer
func (c *partitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

// Errors implements sarama.PartitionConsumer
func (c *partitionConsumer) Errors() <-chan *sarama.ConsumerError {
	// TODO: Add option to pass consumer errors to an errors channel.
	return nil
}

// HighWaterMarkOffset implements sarama.PartitionConsumer
func (c *partitionConsumer) HighWaterMarkOffset() int64 {
	return c.topic.highwaterMark()
}

// catchup makes the consumer go into "catchup" mode, where messages are read
// from the database instead of directly from producers.
// Once the consumer is up to date, i.e. no new messages in the database, then
// the consumer will go back into normal mode where new messages are written
// directly to the channel.
// Must be called with the c.topic.mutex lock
func (c *partitionConsumer) catchup(fromOffset int64) {
	// If we're already in catchup mode or up to date, noop
	if c.catchingUp || fromOffset == c.topic.nextOffset {
		return
	}

	c.catchingUp = true

	// Due to the checks above there can only be one of these goroutines
	// running at a time
	go func() {
		for {
			// Check if we're up to date yet. If we are we exit catchup mode.
			c.topic.mutex.Lock()
			nextOffset := c.topic.nextOffset
			if fromOffset == nextOffset {
				c.catchingUp = false
				c.topic.mutex.Unlock()
				return
			}
			c.topic.mutex.Unlock()

			// Limit the number of messages we request from the database to be the
			// capacity of the channel.
			if nextOffset > fromOffset+int64(cap(c.messages)) {
				nextOffset = fromOffset + int64(cap(c.messages))
			}
			// Fetch the messages from the database.
			msgs, err := c.topic.db.FetchMessages(c.topic.topicName, fromOffset, nextOffset)
			if err != nil {
				// TODO: Add option to write consumer errors to an errors channel
				// as an alternative to logging the errors.
				log.Print("Error reading messages: ", err)
				// Wait before retrying.
				// TODO: Maybe use an exponentional backoff scheme here.
				// TODO: This timeout should take account of all the other goroutines
				// that might be doing the same thing. (If there are a 10000 consumers
				// then we don't want to end up retrying every millisecond)
				time.Sleep(10 * time.Second)
				continue
			}
			if len(msgs) == 0 {
				// This should only happen if the database is corrupted and has lost the
				// messages between the requested offsets.
				log.Fatalf("Corrupt database returned no messages between %d and %d", fromOffset, nextOffset)
			}

			// Pass the messages into the consumer channel.
			// Blocking each write until the channel has enough space for the message.
			for i := range msgs {
				c.messages <- msgs[i].ConsumerMessage(c.topic.topicName)
			}
			// Update our the offset for the next loop iteration.
			fromOffset = msgs[len(msgs)-1].Offset + 1
		}
	}()
}

// notifyNewMessage tells the consumer about a new message
// Must be called with the c.topic.mutex lock
func (c *partitionConsumer) notifyNewMessage(cmsg *sarama.ConsumerMessage) {
	// If we're in "catchup" mode then the catchup routine will send the
	// message later, since cmsg has already been written to the database
	if c.catchingUp {
		return
	}

	// Otherwise, lets try writing the message directly to the channel
	select {
	case c.messages <- cmsg:
	default:
		// The messages channel has filled up, so lets go into catchup
		// mode. Once the channel starts being read from again messages
		// will be read from the database
		c.catchup(cmsg.Offset)
	}
}

type topic struct {
	db        storage.Database
	topicName string
	mutex     sync.Mutex
	consumers []*partitionConsumer
	// nextOffset is the offset that will be assigned to the next message in
	// this topic, i.e. one greater than the last message offset.
	nextOffset int64
}

// send writes messages to a topic.
func (t *topic) send(now time.Time, pmsgs []*sarama.ProducerMessage) error {
	var err error
	// Encode the message keys and values.
	msgs := make([]types.Message, len(pmsgs))
	for i := range msgs {
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

		msgs[i].Headers = pmsgs[i].Headers
	}
	// Take the lock before assigning the offsets.
	t.mutex.Lock()
	defer t.mutex.Unlock()
	offset := t.nextOffset
	for i := range msgs {
		pmsgs[i].Offset = offset
		msgs[i].Offset = offset
		offset++
	}
	// Store the messages while we hold the lock.
	err = t.db.StoreMessages(t.topicName, msgs)
	if err != nil {
		return err
	}
	t.nextOffset = offset

	// Now notify the consumers about the messages.
	for _, msg := range msgs {
		cmsg := msg.ConsumerMessage(t.topicName)
		for _, c := range t.consumers {
			c.notifyNewMessage(cmsg)
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
	// Handle special offsets.
	if offset == sarama.OffsetNewest {
		offset = t.nextOffset
	}
	if offset == sarama.OffsetOldest {
		offset = 0
	}
	c.messages = make(chan *sarama.ConsumerMessage, channelSize)
	t.consumers = append(t.consumers, c)

	// If we're not streaming from the latest offset we need to go into
	// "catchup" mode
	if offset != t.nextOffset {
		c.catchup(offset)
	}
	return c
}

func (t *topic) highwaterMark() int64 {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.nextOffset
}
