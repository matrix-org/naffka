package naffka

import (
	"strconv"
	"testing"
	"time"

	"github.com/matrix-org/naffka/storage/sqlite3"

	sarama "github.com/Shopify/sarama"
)

func MustOpenSQLiteDatabase(t *testing.T) *Naffka {
	db, err := sqlite3.NewDatabase("file::memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.DB.SetMaxOpenConns(1)
	naffka, err := New(db)
	if err != nil {
		t.Fatal(err)
	}
	return naffka
}

func MustOpenInMemoryDatabase(t *testing.T) *Naffka {
	naffka, err := New(&MemoryDatabase{})
	if err != nil {
		t.Fatal(err)
	}
	return naffka
}

func TestSQLiteSendAndReceive(t *testing.T) {
	testSendAndReceive(t, MustOpenSQLiteDatabase(t))
}

func TestSQLiteDelayedReceive(t *testing.T) {
	testDelayedReceive(t, MustOpenSQLiteDatabase(t))
}

func TestSQLiteCatchup(t *testing.T) {
	testCatchup(t, MustOpenSQLiteDatabase(t))
}

func TestSQLiteChannelSaturation(t *testing.T) {
	testChannelSaturation(t, MustOpenSQLiteDatabase(t))
}

func TestInMemorySendAndReceive(t *testing.T) {
	testSendAndReceive(t, MustOpenInMemoryDatabase(t))
}

func TestInMemoryDelayedReceive(t *testing.T) {
	testDelayedReceive(t, MustOpenInMemoryDatabase(t))
}

func TestInMemoryCatchup(t *testing.T) {
	testCatchup(t, MustOpenInMemoryDatabase(t))
}

func TestInMemoryChannelSaturation(t *testing.T) {
	testChannelSaturation(t, MustOpenInMemoryDatabase(t))
}

func testSendAndReceive(t *testing.T, naffka *Naffka) {
	producer := sarama.SyncProducer(naffka)
	consumer := sarama.Consumer(naffka)
	const topic = "testTopic"
	const value = "Hello, World"

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	message := sarama.ProducerMessage{
		Value: sarama.StringEncoder(value),
		Topic: topic,
	}

	if _, _, err = producer.SendMessage(&message); err != nil {
		t.Fatal(err)
	}

	var result *sarama.ConsumerMessage
	select {
	case result = <-c.Messages():
	case <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result.Value) != value {
		t.Fatalf("wrong value: wanted %q got %q", value, string(result.Value))
	}

	select {
	case <-c.Messages():
		t.Fatal("expected to only receive one message")
	default:
	}
}

func testDelayedReceive(t *testing.T, naffka *Naffka) {
	producer := sarama.SyncProducer(naffka)
	consumer := sarama.Consumer(naffka)
	const topic = "testTopic"
	const value = "Hello, World"

	message := sarama.ProducerMessage{
		Value: sarama.StringEncoder(value),
		Topic: topic,
	}

	if _, _, err := producer.SendMessage(&message); err != nil {
		t.Fatal(err)
	}

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	var result *sarama.ConsumerMessage
	select {
	case result = <-c.Messages():
	case <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result.Value) != value {
		t.Fatalf("wrong value: wanted %q got %q", value, string(result.Value))
	}
}

func testCatchup(t *testing.T, naffka *Naffka) {
	producer := sarama.SyncProducer(naffka)
	consumer := sarama.Consumer(naffka)

	const topic = "testTopic"
	const value = "Hello, World"

	message := sarama.ProducerMessage{
		Value: sarama.StringEncoder(value),
		Topic: topic,
	}

	if _, _, err := producer.SendMessage(&message); err != nil {
		t.Fatal(err)
	}

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	var result *sarama.ConsumerMessage
	select {
	case result = <-c.Messages():
	case <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result.Value) != value {
		t.Fatalf("wrong value: wanted %q got %q", value, string(result.Value))
	}

	currOffset := result.Offset

	const value2 = "Hello, World2"
	const value3 = "Hello, World3"

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(value2),
		Topic: topic,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Value: sarama.StringEncoder(value3),
		Topic: topic,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Streaming from %q", currOffset+1)

	c2, err := consumer.ConsumePartition(topic, 0, currOffset+1)
	if err != nil {
		t.Fatal(err)
	}

	var result2 *sarama.ConsumerMessage
	select {
	case result2 = <-c2.Messages():
	case <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result2.Value) != value2 {
		t.Fatalf("wrong value: wanted %q got %q", value2, string(result2.Value))
	}
}

func testChannelSaturation(t *testing.T, naffka *Naffka) {
	// The channel returned by c.Messages() has a fixed capacity
	producer := sarama.SyncProducer(naffka)
	consumer := sarama.Consumer(naffka)
	const topic = "testTopic"
	const baseValue = "testValue: "

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	channelSize := cap(c.Messages())

	// We want to send enough messages to fill up the channel, so lets double
	// the size of the channel. And add three in case its a zero sized channel
	numberMessagesToSend := 2*channelSize + 3

	var sentMessages []string

	for i := 0; i < numberMessagesToSend; i++ {
		value := baseValue + strconv.Itoa(i)

		message := sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(value),
		}

		sentMessages = append(sentMessages, value)

		if _, _, err = producer.SendMessage(&message); err != nil {
			t.Fatal(err)
		}
	}

	var result *sarama.ConsumerMessage

	j := 0
	for ; j < numberMessagesToSend; j++ {
		select {
		case result = <-c.Messages():
		case <-time.NewTimer(10 * time.Second).C:
			t.Fatalf("failed to receive message %d out of %d", j+1, numberMessagesToSend)
		}

		expectedValue := sentMessages[j]
		if string(result.Value) != expectedValue {
			t.Fatalf("wrong value: wanted %q got %q", expectedValue, string(result.Value))
		}
	}

	select {
	case <-c.Messages():
		t.Fatalf("expected to only receive %d messages", numberMessagesToSend)
	default:
	}
}
