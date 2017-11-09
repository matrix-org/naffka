package naffka

import (
	"testing"
	"time"

	sarama "gopkg.in/Shopify/sarama.v1"
)

func TestSendAndReceive(t *testing.T) {
	naffka, err := New(&MemoryDatabase{})
	if err != nil {
		t.Fatal(err)
	}
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
	case _ = <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result.Value) != value {
		t.Fatalf("wrong value: wanted %q got %q", value, string(result.Value))
	}

	select {
	case result = <-c.Messages():
		t.Fatal("expected to only receive one message")
	default:
	}
}

func TestDelayedReceive(t *testing.T) {
	naffka, err := New(&MemoryDatabase{})
	if err != nil {
		t.Fatal(err)
	}
	producer := sarama.SyncProducer(naffka)
	consumer := sarama.Consumer(naffka)
	const topic = "testTopic"
	const value = "Hello, World"

	message := sarama.ProducerMessage{
		Value: sarama.StringEncoder(value),
		Topic: topic,
	}

	if _, _, err = producer.SendMessage(&message); err != nil {
		t.Fatal(err)
	}

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	var result *sarama.ConsumerMessage
	select {
	case result = <-c.Messages():
	case _ = <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result.Value) != value {
		t.Fatalf("wrong value: wanted %q got %q", value, string(result.Value))
	}
}

func TestCatchup(t *testing.T) {
	naffka, err := New(&MemoryDatabase{})
	if err != nil {
		t.Fatal(err)
	}
	producer := sarama.SyncProducer(naffka)
	consumer := sarama.Consumer(naffka)

	const topic = "testTopic"
	const value = "Hello, World"

	message := sarama.ProducerMessage{
		Value: sarama.StringEncoder(value),
		Topic: topic,
	}

	if _, _, err = producer.SendMessage(&message); err != nil {
		t.Fatal(err)
	}

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	var result *sarama.ConsumerMessage
	select {
	case result = <-c.Messages():
	case _ = <-time.NewTimer(10 * time.Second).C:
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
	case _ = <-time.NewTimer(10 * time.Second).C:
		t.Fatal("expected to receive a message")
	}

	if string(result2.Value) != value2 {
		t.Fatalf("wrong value: wanted %q got %q", value2, string(result2.Value))
	}
}
