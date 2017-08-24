package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

func TestMain(m *testing.M) {
	cfg.Port = 6382

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		main()
		wg.Done()
	}()

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
	shutdown <- os.Interrupt
	wg.Wait()
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

func TestErrRPUSHX(t *testing.T) {
	c := newClient("some:producer")

	_, err := c.RPushX("invalid-arg", "a msg").Result()
	if err == nil {
		t.Error("Expected error, got nothing")
	}

	err = c.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestSETNAME(t *testing.T) {
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

func newClient(id string) *redis.Client {
	return redis.NewClient(&redis.Options{
		// TODO Add the ability to read host and port from a cfg file
		Addr:      fmt.Sprintf("%s:%d", "localhost", 6382),
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
