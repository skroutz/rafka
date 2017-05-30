package main

import "time"
import rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"

type Config struct {
	Librdkafka  rdkafka.ConfigMap `json:"librdkafka"`
	CommitIntvl time.Duration
}
