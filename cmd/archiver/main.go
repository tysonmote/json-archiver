package main

import (
	"context"
	"flag"
	"strings"

	kafka "github.com/segmentio/kafka-go"
	"github.com/tidwall/gjson"
)

var (
	kafkaBrokers = flag.String("kafka", "localhost:9092", "Comma-separated list of Kafka brokers, as \"host:port\"")
	kafkaGroupID = flag.String("kafka-group-id", "json-archiver", "Kafka consumer group ID")
	kafkaTopic   = flag.String("kafka-topic", "events", "Kafka topic to consume from")
	groupKeys    = flag.String("group-keys", "projectId,collection", "Comma-separated list of JSON keys to use as the grouping key")
)

func main() {
	flag.Parse()
	groupByKeys := strings.Split(*groupKeys, "")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(*kafkaBrokers, ","),
		GroupID:  *kafkaGroupID,
		Topic:    *kafkaTopic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	ctx := context.Background()
	var m kafka.Message
	var err error

	for {
		m, err = r.FetchMessage(ctx)
		if err != nil {
			panic(err)
		}

		// Get key
		results := gjson.GetManyBytes(m, groupByKeys...)
		keyParts := make([]string, len(results))
		for i := range results {
			keyParts[i] = results[i].String()
		}
		key := strings.Join(keyParts, "/")

		// fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	// err = m.CommitMessages(ctx, m)
	// if err != nil {
	//   panic(err)
	// }
}

// loop:
//   for N minutes:
//     for each message:
//       accumulate in file per partition key
//
//   then:
//     flush all to S3 with start / end offset, size, count
//     update postgres in single transaction
//     update kafka offset
//
// TODO: how to handle adding more partitions?
// TODO: how to handle crashes?
