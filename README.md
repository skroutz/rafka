# rafka - Kafka with a Redis API

`rafka` acts a gateway service and exposes Kafka using the redis protocol.

## Status

This project is under heavy development and the [protocol](https://github.skroutz.gr/skroutz/rafka/wiki/Protocol) is still in draft version.

Why rafka?
----------


- Hide Kafka low-level details from the application & provide sane defaults.
- Use a Redis client instead of a Kafka client. This particularly useful
  in languages that lack a proper Kafka driver, or do not provide
  concurrency primitives to implement buffering & other optimizations.

Dependencies
------------

- `golang-github-confluentinc-confluent-kafka-go-dev` (go librdkafka bindings)
- `golang-github-urfave-cli-dev`

Installation
------------

```shell
$ apt-get install golang librdkafka-dev
$ go get -u golang.skroutz.gr/skroutz/rafka
```

Consumer
--------

Kafka is designed in a way that each consumer represents a worker processing
Kafka messages, that worker sends heartbeats and is depooled from its group
when it missbehaves. Those semantics are preserved in `rafka` by using
**stateful connections**. In `rafka` a connection is tied with a set of Kafka
consumers.  Consumers are not shared between connections and, once the
connection closes, the consumers are destroyed too.

Each connection first needs to identify itself by using `client setname
<group.id>:<name>` and then it can begin processing messages by issuing `blpop`
calls on the desired topics. Each message should be explicitly acknowledged
so it can be commited to Kafka. Acks are `rpush`ed to the special `acks` key.

Redis Protocol
--------------

`rakfa` tries to expose a redis protocol keeping Redis semantics where
applicable. We also try to design the protocol in a way that `rafka` can be
substituted by a plain Redis instance so it is easier to test client code &
libraries.

```
COMMANDS:

client setname <group.id>:<name>
client getname
blpop topics:<topics> <timeout>
rpush acks <topic>:<partition>:<offset>
```

Sample Redis Flow
-----------------

```
$ redis-cli -p 6380
127.0.0.1:6380> client setname group1:jo
"OK"
127.0.0.1:6380> client getname
"group1:jo"
127.0.0.1:6380> blpop topics:mytopic 3
1) "topic"
2) "mytopic"
3) "partition"
4) (integer) 0
5) "offset"
6) (integer) 2275532
7) "value"
8) "payload"
127.0.0.1:6380> blpop topics:mytopic 3
(nil)
(3.00s)
127.0.0.1:6380> rpush acks mytopic:0:2275532
"OK"
```

Usage
-----

```shell
$ vim kafka.cfg # Add brokers
$ go build && ./rafka -k ./kafka.cfg

```

TODO
----

- Finalize Consumer
- Investigate a slow consumer scenario
- Implement a thin Ruby Client on top of Redis
- Proper tests
- Implement Producer
