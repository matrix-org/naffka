package naffka

import (
	"fmt"
	"sync"
)

// A MemoryDatabase stores the message history as arrays in memory.
// It can be used to run unit tests.
// If the process is stopped then any messages that haven't been
// processed by a consumer are lost forever.
type MemoryDatabase struct {
	topicsMutex sync.Mutex
	topics      map[string]*memoryDatabaseTopic
}

type memoryDatabaseTopic struct {
	messagesMutex sync.Mutex
	messages      []Message
}

func (t *memoryDatabaseTopic) addMessage(msg *Message) {
	t.messagesMutex.Lock()
	defer t.messagesMutex.Unlock()
	for int64(len(t.messages)) < msg.Offset {
		// Pad out the array with empty messages if the offset is bigger than
		// the end of the array. This probably isn't necessary, since usually
		// the messages are assigned sequential contiguous offsets. However until
		// we commit to that restriction we should support out of order stores.
		t.messages = append(t.messages, Message{})
	}
	if int64(len(t.messages)) == msg.Offset {
		t.messages = append(t.messages, *msg)
	} else {
		// Handle out of order stores. This probably isn't necessary, since
		// the usually the messages are assigned sequential contiguous offsets.
		// Until we commit to that restriction we support out of order stores.
		t.messages[msg.Offset] = *msg
	}
}

func (t *memoryDatabaseTopic) getMessages() []Message {
	t.messagesMutex.Lock()
	defer t.messagesMutex.Unlock()
	return t.messages
}

func (m *MemoryDatabase) getTopic(topicName string) *memoryDatabaseTopic {
	m.topicsMutex.Lock()
	defer m.topicsMutex.Unlock()
	result := m.topics[topicName]
	if result == nil {
		result = &memoryDatabaseTopic{}
		if m.topics == nil {
			m.topics = map[string]*memoryDatabaseTopic{}
		}
		m.topics[topicName] = result
	}
	return result
}

// StoreMessages implements Database
func (m *MemoryDatabase) StoreMessages(messages []Message) error {
	for i := range messages {
		m.getTopic(messages[i].Topic).addMessage(&messages[i])
	}
	return nil
}

// FetchMessages implements Database
func (m *MemoryDatabase) FetchMessages(topic string, startOffset, endOffset int64) ([]Message, error) {
	messages := m.getTopic(topic).getMessages()
	if endOffset > int64(len(messages)) {
		return nil, fmt.Errorf("end offset %d out of range %d", endOffset, len(messages))
	}
	if startOffset >= endOffset {
		return nil, fmt.Errorf("start offset %d greater than or equal to end offset %d", startOffset, endOffset)
	}
	if startOffset < -1 {
		return nil, fmt.Errorf("start offset %d less than -1", startOffset)
	}
	return messages[startOffset+1 : endOffset], nil
}

// MaxOffsets implements Database
func (m *MemoryDatabase) MaxOffsets() (map[string]int64, error) {
	m.topicsMutex.Lock()
	defer m.topicsMutex.Unlock()
	result := map[string]int64{}
	for name, t := range m.topics {
		result[name] = int64(len(t.getMessages())) - 1
	}
	return result, nil
}
