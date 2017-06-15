package main

import "time"
import rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"

type Config struct {
	Librdkafka struct {
		General  rdkafka.ConfigMap `json:"general"`
		Consumer rdkafka.ConfigMap `json:"consumer"`
		Producer rdkafka.ConfigMap `json:"producer"`
	}

	CommitIntvl time.Duration
}
