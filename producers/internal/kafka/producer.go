package kafka

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/hamba/avro/v2"
)

type TillProducer struct {
	producer *kafka.Producer
	SRClient *SchemaRegistryClient
}

func NewTillProducer(brokerURL, srURL string) (*TillProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            brokerURL,
		"linger.ms":                    5,
		"batch.num.messages":           10000,
		"queue.buffering.max.messages": 2000000,
	})
	if err != nil {
		return nil, err
	}

	// Drain delivery reports asynchronously to prevent queue backup
	go func() {
		for e := range p.Events() {
			_ = e
		}
	}()

	return &TillProducer{
		producer: p,
		SRClient: &SchemaRegistryClient{BaseURL: srURL},
	}, nil
}

// EncodeAvroWithMagicByte prepends the 5-byte Confluent wire-format header
func EncodeAvroWithMagicByte(schemaID int, avroBytes []byte) []byte {
	header := make([]byte, 5)
	header[0] = 0 // Magic byte
	binary.BigEndian.PutUint32(header[1:], uint32(schemaID))
	return append(header, avroBytes...)
}

// ProduceMessage serializes the struct to Avro and publishes to Kafka
func (tp *TillProducer) ProduceMessage(topic string, key string, schemaID int, avroSchema avro.Schema, value interface{}) error {
	avroBytes, err := avro.Marshal(avroSchema, value)
	if err != nil {
		return err
	}

	finalPayload := EncodeAvroWithMagicByte(schemaID, avroBytes)

	err = tp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key), // TILL-08 (Phase 2): Keying by TenantID for partitioning
		Value:          finalPayload,
		Headers: []kafka.Header{
			{Key: "generation_time_ms", Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10))},
		},
	}, nil) // Asynchronous delivery
	
	return err
}

func (tp *TillProducer) Close() {
	tp.producer.Flush(15 * 1000)
	tp.producer.Close()
}
