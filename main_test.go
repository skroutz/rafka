package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agis/spawn"
	rdkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis"
)

func TestMain(m *testing.M) {
	cmd := spawn.New(main, "-p", "6383", "-c", "test/librdkafka.test.json")

	// start rafka
	ctx, cancel := context.WithCancel(context.Background())
	err := cmd.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}

	c := newClient("wait:for-rafka")
	serverReady := false
	for i := 0; i <= 3 && !serverReady; i++ {
		serverReady = c.Ping().Val() == "PONG"
		time.Sleep(300 * time.Millisecond)
	}

	if !serverReady {
		log.Fatal("Server not ready in time")
		os.Exit(1)
	}

	result := m.Run()

	cancel()
	err = cmd.Wait()
	if err != nil {
		log.Fatal(err)
	}

	os.Exit(result)
}

// TODO(agis) Move this to an end-to-end test when rafka-rb is able to consume
// multiple topics from the same client instance.
func TestConsumerTopicExclusive(t *testing.T) {
	id := "bar:baz"
	c := newClient(id)
	_, err := c.BLPop(1*time.Second, "topics:foo").Result()
	if err != nil && err != redis.Nil {
		t.Fatal(err)
	}

	expErr := "CONS Topic foo has another consumer"
	_, err = c.BLPop(1*time.Second, "topics:foo,foo2").Result()
	if err.Error() != expErr {
		t.Errorf("Expected error: `%s`, was `%s`", expErr, err)
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestConsumerConfig(t *testing.T) {
	c := newClient("foo:bar1")
	_, err := c.BLPop(1*time.Second, `topics:foo:{"auto.offset.reset": "latest"}`).Result()
	if err != nil && err != redis.Nil {
		t.Fatal(err)
	}

	// configuration only applies in the 1st operation
	_, err = c.BLPop(1*time.Second, `topics:foo:{"auto.offset.reset": "malformed"}`).Result()
	if err != nil && err != redis.Nil {
		t.Fatal(err)
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}

	c = newClient("foo2:bar2")
	_, err = c.BLPop(1*time.Second, `topics:foo:{"invalid_option":"1"}`).Result()
	if err == nil {
		t.Fatal("Expected invalid configuration error, got nothing")
	}
}

// RPUSH
func TestConsumerOffsetCommit(t *testing.T) {
	cases := [][]string{
		{"acks", "sometopic:1:-5"},
		{"acks", "sometopic:1:foo"},
		{"acks", "sometopic:10"},
		{"acks", "sometopic"},
		{"wrongkey", "sometopic:1:5"},
	}

	c := newClient("foo:offset")
	// spawn the consumer
	c.BLPop(1*time.Second, "topics:sometopic").Result()

	for _, args := range cases {
		_, err := c.RPush(args[0], args[1]).Result()
		if err == nil {
			t.Errorf("Expected error for `%v %v`, got nothing", args[0], args[1])
		}
	}
}

// RPUSHX
func TestProduceErr(t *testing.T) {
	c := newClient("some:producer:" + t.Name())

	_, err := c.RPushX("invalid-arg", "a msg").Result()
	if err == nil {
		t.Error("Expected error, got nothing")
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestProduceWithKey(t *testing.T) {
	c := newClient("some:producer:" + t.Name())

	_, err := c.RPushX("topic:foo:bar", "a msg").Result()
	if err != nil {
		t.Error(err)
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}
}

// SETNAME
func TestClientID(t *testing.T) {
	numReq := 100
	replies := make(chan string)

	expected := 0
	for i := 0; i < numReq; i++ {
		expected += i
		go func(n int) {
			c := newClient((fmt.Sprintf("foo:bar-%d", n)))

			res, err := c.ClientGetName().Result()
			if err != nil {
				t.Error(err)
			}

			replies <- res

			err = c.Close()
			if err != nil {
				t.Error(err)
			}
		}(i)
	}

	actual := 0
	for i := 0; i < numReq; i++ {
		n, err := strconv.Atoi(strings.Split(<-replies, "-")[1])
		if err != nil {
			t.Fatal(err)
		}
		actual += n
	}

	if actual != expected {
		t.Errorf("Expected %d, got %d", expected, actual)
	}
}

func TestConcurrentProducers(t *testing.T) {
	var wg sync.WaitGroup
	numProd := 5

	wg.Add(numProd)

	for i := 0; i < numProd; i++ {
		go func(n int) {
			defer wg.Done()

			c := newClient(fmt.Sprintf("some:producer-%d", n))

			c.RPushX("topic:foo", "a msg").Result()

			err := c.Close()
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
}

// HGETALL
func TestStatsQuery(t *testing.T) {
	p := newClient("someone:foo")
	v, err := p.HGetAll("stats").Result()
	if err != nil {
		t.Error(err)
	}

	_, err = strconv.Atoi(v["producer.delivery.errors"])
	if err != nil {
		t.Error(err)
	}

	_, err = strconv.Atoi(v["producer.unflushed.messages"])
	if err != nil {
		t.Error(err)
	}
}

func TestParseTopics(t *testing.T) {
	cases := map[string][]string{
		"topics:bar":      {"bar"},
		"topics:foo,bar":  {"foo", "bar"},
		"topics:baz:":     {"baz"},
		"topics:baz,foo:": {"baz", "foo"},
	}

	for input, expected := range cases {
		actual, _, err := parseTopicsAndConfig(input)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

func TestParseConfig(t *testing.T) {
	cases := map[string]rdkafka.ConfigMap{
		`topics:bar:{"cfg":"foo"}`:             {"cfg": "foo"},
		`topics:bar:{"a":"b","c":"d","e":"f"}`: {"a": "b", "c": "d", "e": "f"},
	}

	for input, expected := range cases {
		_, actual, err := parseTopicsAndConfig(input)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("Expected %v, got %v", expected, actual)
		}
	}
}

// KEYS
func TestMetadataQuery(t *testing.T) {
	c := newClient("someone:random_producer")

	_, err := c.Keys("topics:").Result()
	if err != nil {
		t.Errorf("Could not execute KEYS command: `%s`", err)
	}
}

func newClient(id string) *redis.Client {
	return redis.NewClient(&redis.Options{
		// TODO Add the ability to read host and port from a cfg file
		Addr:      fmt.Sprintf("%s:%d", "localhost", 6383),
		OnConnect: setName(id)})
}

func setName(id string) func(*redis.Conn) error {
	return func(c *redis.Conn) error {
		res := c.ClientSetName(id)
		if res.Err() != nil {
			log.Fatalf("%s", res.Err())
		}
		return nil
	}
}
