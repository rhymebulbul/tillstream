package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"tillstream/producers/internal/generator"
	"tillstream/producers/internal/kafka"

	"github.com/hamba/avro/v2"
)

const orderSchemaStr = `
{
  "type": "record",
  "name": "Order",
  "namespace": "com.tillstream.pos",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "tenant_id", "type": "string"},
    {"name": "store_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "loyalty_points", "type": "int", "default": 0},
    {"name": "total_price", "type": "double"},
    {"name": "created_at", "type": "string"}
  ]
}`

const paymentSchemaStr = `
{
  "type": "record",
  "name": "Payment",
  "namespace": "com.tillstream.pos",
  "fields": [
    {"name": "payment_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "tenant_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "payment_method", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "created_at", "type": "string"}
  ]
}`

func main() {
	fmt.Println("Starting TillStream POS Producer with Kafka & Avro...")

	brokerURL := os.Getenv("KAFKA_BROKER")
	if brokerURL == "" {
		brokerURL = "broker:29092" // Docker compose internal network name
	}

	srURL := os.Getenv("SCHEMA_REGISTRY_URL")
	if srURL == "" {
		srURL = "http://schema-registry:8081" // Docker compose internal network name
	}

	producer, err := kafka.NewTillProducer(brokerURL, srURL)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Parse Avro Schemas
	orderAvro, err := avro.Parse(orderSchemaStr)
	if err != nil {
		log.Fatalf("Invalid Order Schema: %v", err)
	}

	paymentAvro, err := avro.Parse(paymentSchemaStr)
	if err != nil {
		log.Fatalf("Invalid Payment Schema: %v", err)
	}

	// Register Schemas
	orderSchemaID, err := producer.SRClient.RegisterSchema("orders-value", orderSchemaStr)
	if err != nil {
		log.Fatalf("Failed to register order schema: %v", err)
	}

	paymentSchemaID, err := producer.SRClient.RegisterSchema("payments-value", paymentSchemaStr)
	if err != nil {
		log.Fatalf("Failed to register payment schema: %v", err)
	}

	fmt.Printf("Registered Schemas - Orders ID: %d, Payments ID: %d\n", orderSchemaID, paymentSchemaID)

	concurrency := 1000
	fmt.Printf("Starting benchmark with %d concurrent workers...\n", concurrency)

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			localGen := generator.NewGenerator(int64(workerID) + time.Now().UnixNano())
			for {
				order, payment := localGen.GenerateOrderFlow()

				// Produce Order
				err = producer.ProduceMessage("orders", order.TenantID, orderSchemaID, orderAvro, order)
				if err != nil {
					log.Printf("Worker %d Failed to produce order: %v\n", workerID, err)
				}

				// Produce Payment
				err = producer.ProduceMessage("payments", payment.TenantID, paymentSchemaID, paymentAvro, payment)
				if err != nil {
					log.Printf("Worker %d Failed to produce payment: %v\n", workerID, err)
				}
			}
		}(i)
	}

	// Keep main thread alive
	select {}
}
