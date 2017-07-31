package main

import (
	"fmt"
	"testing"

	"github.com/go-redis/redis"
)

func TestSetIDTwice(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		OnConnect: func(c *redis.Conn) error {
			res := c.ClientSetName("foobar:foo2")
			if res.Err() != nil {
				t.Fatal(res.Err())
			}

			if c.ClientSetName("foobar:foo3").Err() == nil {
				t.Fatal("Expected error, got nothing")
			}
			return nil
		}})

	_, err := c.Ping().Result()
	if err != nil {
		t.Fatal(err)
	}
}
