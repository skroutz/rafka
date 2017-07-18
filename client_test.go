package main

import (
	"testing"

	"github.com/go-redis/redis"
)

func TestSetIDTwice(t *testing.T) {
	c := redis.NewClient(&redis.Options{
		Addr: "localhost:6380",
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

	c.Ping().Result()
}
