rafka
=====

[![Build Status](https://api.travis-ci.org/skroutz/rafka.svg?branch=master)](https://travis-ci.org/skroutz/rafka)
[![Go report](https://goreportcard.com/badge/github.com/skroutz/rafka)](https://goreportcard.com/report/github.com/skroutz/rafka)
[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

#### Table of Contents

- [Rationale](#rationale)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Design](#design)
    - [Protocol](#protocol)
    - [Consumer](#consumer)
    - [Producer](#producer)
- [API](#api)
    - [Protocol](#protocol)
    - [Consumer](#consumer)
    - [Generic](#generic)
- [Client libraries](#client-libraries)
- [Development](#development)
- [License](#license)


rafka is a gateway service that exposes Kafka using simple semantics.

It implements a small subset of the [Redis protocol](https://redis.io/topics/protocol), so that it
can be used by leveraging existing Redis client libraries.

Rationale
---------

Using Kafka with languages that lack a reliable, solid client library can be a problem for
mission-critical applications.

Using rafka we can:

- Hide Kafka low-level details from the application and provide sane defaults, backed by the
  excellent [librdkafka](https://github.com/edenhill/librdkafka).
- Use a Redis client instead of a Kafka client. This is particularly useful in languages that lack a
  proper Kafka client library or do not provide concurrency primitives to implement buffering and
  other optimizations. Furthermore, writing a rafka client is much easier than writing a Kafka
  client. For a list of available client libraries see [_Client libraries_](#client-libraries).

Refer to [*"Introducing Kafka to a Rails application"*](https://engineering.skroutz.gr/blog/kafka-rails-integration/)
for more background and how rafka is used in a production environment.

Requirements
------------

- [librdkafka](https://github.com/edenhill/librdkafka) 0.11.5 or later
- A Kafka cluster

Getting Started
------------

1. Install [librdkafka](https://github.com/edenhill/librdkafka):

```shell
# debian
$ sudo apt-get install librdkafka-dev

# macOS
$ brew install librdkafka
```

2. Install rafka:

```shell
$ go get -u github.com/skroutz/rafka
```

3. Run it:

```shell
$ rafka -c librdkafka.json.sample
[rafka] 2017/06/26 11:07:23 Spawning Consumer Manager (librdkafka 0.11.0)...
[server] 2017/06/26 11:07:23 Listening on 0.0.0.0:6380
```

Design
------

### Protocol

rafka exposes a subset of the [Redis protocol](https://redis.io/topics/protocol) and tries to keep
Redis semantics where possible.

We also try to design the protocol in a way that rafka can be replaced by a plain Redis instance so
that it's easier to test client code and libraries.

### Consumer

In Kafka, each consumer represents a worker processing messages. That worker sends heartbeats and
is de-pooled from its group when it misbehaves.

Those semantics are preserved in rafka by using _stateful connections_. In rafka, each connection
is tied with a set of Kafka consumers. Consumers are not shared between connections and once the
connection closes, the respective consumers are gracefully shut down too.

Each consumer must identify itself upon connection, by using `client setname <group.id>:<name>`.
Then it can begin processing messages by issuing `blpop` calls on the desired topics. Each message
should be explicitly acknowledged so it can be committed to Kafka. Acks are `rpush`ed to the
special `acks` key.

For more info refer to [API - Consumer](https://github.com/skroutz/rafka#consumer-1).

#### Caveats

rafka periodically calls [`Consumer.StoreOffsets()`](https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#Consumer.StoreOffsets)
under the hood. This means consumers must be configured accordingly:

- `enable.auto.commit` must be set to `true`
- `enable.auto.offset.store` [must](https://github.com/edenhill/librdkafka/blob/v0.11.4/src/rdkafka.h#L2665) be set to `false`

For more info see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.

### Producer

- Each client connection is tied to a single Producer.
- Producers are not shared between connections and once the connection closes, its producer is also
  shutdown gracefully.
- Producers produce messages using `RPUSHX`.
- Produced messages are buffered in rafka and are eventually flushed to Kafka. However, `DUMP` can
  be used to force a synchronous flush of any outstanding messages.

For more info refer to [API - Producer](https://github.com/skroutz/rafka#producer-1).

#### Caveats

There is currently is an upper message limit of **32MB** to the messages that may be produced. It
is controlled by `go-redisproto.MaxBulkSize`.

API
---

### Producer

- `RPUSHX topics:<topic> <message>` produce a message
- `RPUSHX topics:<topic>:<key> <message>` produce a message with a partition key.  Messages with
  the same key will always be assigned to the same partition.
- `DUMP <timeoutMs>` flush any outstanding messages to Kafka. This is a blocking operation; it
  returns until all buffered messages are flushed or the timeoutMs exceeds

Example using redis-cli:

```shell
127.0.0.1:6380> rpushx topics:greetings "hello there!"
"OK"
```

### Consumer

- `CLIENT SETNAME <group.id>:<name>` sets the consumer group and name
- `CLIENT GETNAME`
- `BLPOP topics:<topic>:<JSON-encoded consumer config> <timeoutMs>` consume the next message from
  topic
- `RPUSH acks <topic>:<partition>:<offset>` commit the offset for the given topic/partition

Example using redis-cli:

```shell
127.0.0.1:6380> client setname myapp:a-consumer
"OK"
127.0.0.1:6380> blpop topics:greetings 1000
1) "topic"
2) "greetings"
3) "partition"
4) (integer) 2
5) "offset"
6) (integer) 10
7) "value"
8) "hello there!"
# ... do some work with the greeting...
127.0.0.1:6380> rpush acks greetings:2:10
"OK"
```

### Generic

- [`PING`](https://redis.io/commands/ping)
- [`QUIT`](https://redis.io/commands/quit)
- [`MONITOR`](https://redis.io/commands/monitor)
- `HGETALL stats` get monitoring statistics monitoring
- `KEYS topics:` list all topics
- `DEL stats` reset the monitoring statistics

Client libraries
----------------

- Ruby: [rafka-rb](https://github.com/skroutz/rafka-rb)

Development
-----------

If this is your first time setting up development on rafka, ensure that you have all the build
dependencies via [dep](https://github.com/golang/dep):

```shell
$ dep ensure
```

To run all the tests (Go + end-to-end) do:

```shell
$ DIST=buster RDKAFKA_VERSION=v1.2.1 make test
```

License
-------

rafka is released under the GNU General Public License version 3. See [COPYING](COPYING).
