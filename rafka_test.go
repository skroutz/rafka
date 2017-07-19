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

	for newClient("wait:for-rafka").Ping().Val() != "PONG" {
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
				t.Fatal(err)
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

func newClient(id string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:      fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
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
