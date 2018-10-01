Rafka
==============================
[![Build Status](https://api.travis-ci.org/skroutz/rafka.svg?branch=master)](https://travis-ci.org/skroutz/rafka)
[![Go report](https://goreportcard.com/badge/github.com/skroutz/rafka)](https://goreportcard.com/report/github.com/skroutz/rafka)
[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Rafka is a gateway service that exposes Kafka using simple semantics.

It implements a small subset of the [Redis protocol](https://redis.io/topics/protocol), so that it can be used by leveraging existing Redis client libraries.





Rationale
-------------------------------------------------------------------------------
Using Kafka with languages that lack a reliable, solid client library can be a
problem for mission-critical applications.

Using Rafka we can:

- Hide Kafka low-level details from the application and provide sane defaults,
  backed by the excellent [librdkafka](https://github.com/edenhill/librdkafka).
- Use a Redis client instead of a Kafka client. This particularly useful
  in languages that lack a proper Kafka client library or do not provide
  concurrency primitives to implement buffering and other optimizations. Furthermore,
  writing a Rafka client is much easier than writing a Kafka client. For a
  list of available client libraries see [_Client libraries_](#client-libraries).

Refer to [*"Introducing Kafka to a Rails application"*](https://engineering.skroutz.gr/blog/kafka-rails-integration/)
for more information.







Requirements
-------------------------------------------------------------------------------

- [librdkafka 0.11.0 or later](https://github.com/edenhill/librdkafka)
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
2. Install Rafka:
   ```shell
   $ go get github.com/skroutz/rafka
   ```
3. Run it:
   ```shell
   $ rafka -c librdkafka.json.sample
   [rafka] 2017/06/26 11:07:23 Spawning Consumer Manager (librdkafka 0.11.0)...
   [server] 2017/06/26 11:07:23 Listening on 0.0.0.0:6380
   ```



Design
-------------------------------------------------------------------------------

### Protocol
Rafka exposes a [Redis protocol](https://redis.io/topics/protocol) and tries to
keep Redis semantics where possible.

We also try to design the protocol in a way that Rafka can be
replaced by a plain Redis instance so that it's easier to test client code and
libraries.





### Consumer
Kafka is designed in a way that each consumer represents a worker processing
Kafka messages, that worker sends heartbeats and is depooled from its group
when it misbehaves. Those semantics are preserved in `rafka` by using
**stateful connections**. In `rafka` a connection is tied with a set of Kafka
consumers.  Consumers are not shared between connections and, once the
connection closes, the consumers are destroyed too.

Each connection first needs to identify itself by using `client setname
<group.id>:<name>` and then it can begin processing messages by issuing `blpop`
calls on the desired topics. Each message should be explicitly acknowledged
so it can be committed to Kafka. Acks are `rpush`ed to the special `acks` key.




### Producer
Each client connection is tied to a single Producer.
Producers are not shared between connections and once the connection closes, its
Producer is also destroyed.

Producers can immediately begin producing using `RPUSHX`, without having to call
`CLIENT SETNAME` first.

Since the produced messages are buffered in Rafka and are eventually flushed
to Kafka (eg. when the client connection is closed), `DUMP` can also be used to
force a synchronous flush of the messages.



API
------------------------------------------------------------------------------

### Producer
- `RPUSHX topics:<topic> <message>`: produce a message. The message will be assigned to a random partition.
- `RPUSHX topics:<topic>:<key> <message>`: produce a message with a key. Two or more messages with the same key will always be assigned to the same partition.
- `DUMP <timeoutMs>`: flushes the messages to Kafka. This is a blocking operation, it returns until all buffered messages are flushed or the timeout exceeds

Example using Redis:
```
127.0.0.1:6380> rpushx topics:greetings "hello there!"
"OK"
```





### Consumer
- `CLIENT SETNAME <group.id>:<name>`: sets the consumer's group & name
- `CLIENT GETNAME`
- `BLPOP topics:<topic>:<JSON-encoded consumer config> <timeoutMs>`: consumes the next message from the given topic
- `RPUSH acks <topic>:<partition>:<offset>`: commit the offset for the given topic/partition

Example using Redis:
```
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
- `HGETALL stats`: returns a hash with various statistics useful for
  monitoring.
- `KEYS topics:`: list all topics
- `DEL stats`: resets the monitoring statistics.








Client libraries
-------------------------------------------------------------------------------

- Ruby: [rafka-rb](https://github.com/skroutz/rafka-rb)





Development
-------------------------------------------------------------------------------


If this is your first time setting up development on Rafka, ensure that you
have all the build dependencies via [dep](https://github.com/golang/dep):

```shell
$ dep ensure
```


Running the Go tests:
```shell
$ go test
```

We also have end-to-end tests that run via Docker. Refer
[here](test/README.md) for more information.


Run tests (must have done `make spawn` before), perform various static checks
and finally build the project:
```shell
$ make
```










License
-------------------------------------------------------------------------------
Rafka is released under the GNU General Public License version 3. See [COPYING](COPYING).

