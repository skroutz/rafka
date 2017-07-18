Rafka: Kafka with a Redis API
==============================

Rafka is a gateway service that exposes Kafka using the [Redis protocol](https://redis.io/topics/protocol).





Rationale
-------------------------------------------------------------------------------
Using Kafka with languages that lack a reliable, solid client library can be a
problem for mission-critical applications. At Skroutz we use Ruby and we faced
this problem constantly.

Using Rafka we can:

- Hide Kafka low-level details from the application and provide sane defaults,
  backed by the excellent [librdkafka](https://github.com/edenhill/librdkafka).
- Use a Redis client instead of a Kafka client. This particularly useful
  in languages that lack a proper Kafka driver or do not provide
  concurrency primitives to implement buffering and other optimizations. Furthermore,
  writing a Rafka client is much easier than writing a Kafka client. For a
  Ruby driver see [rafka-rb](https://github.com/skroutz/rafka-rb).




Status
-------------------------------------------------------------------------------
This project is under heavy development and is _not_ recommended for
use in production environments until we reach the [1.x series](https://github.com/skroutz/rafka/milestone/1).




Dependencies
-------------------------------------------------------------------------------
- [Go 1.8 or later](https://golang.org/)
- [librdkafka](https://github.com/edenhill/librdkafka)



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
   $ rafka -k kafka.json.sample
   [rafka] 2017/06/26 11:07:23 Spawning Consumer Manager (librdkafka 0.9.5)...
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


Usage
-------------------------------------------------------------------------------

```shell
$ vim kafka.json.sample # Add brokers
$ go build && ./rafka -k ./kafka.json.sample -i 10
```


API
------------------------------------------------------------------------------
Generic commands:

- [`PING`](https://redis.io/commands/ping) (no arguments)
- [`QUIT`](https://redis.io/commands/quit)






### Producer
- `RPUSHX topics:<topic> <message>`: produce a message
- `DUMP <timeoutMs>`: flushes the messages to Kafka. This is a blocking operation, it returns until all buffered messages are flushed or the timeout exceeds

Example using Redis:
```
127.0.0.1:6380> rpushx topics:greetings "hello there!"
"OK"
```





### Consumer
- `CLIENT SETNAME <group.id>:<name>`: sets the consumer's group & name
- `CLIENT GETNAME`
- `BLPOP topics:<topic> <timeoutMs>`: consumes the next message from the given topic
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







Development
-------------------------------------------------------------------------------


If this is your first time setting up development on Rafka, ensure that you
have all the build dependencies via [dep](https://github.com/golang/dep):

```shell
$ dep ensure
```


Building (also run tests and performs various static checks):
```shell
$ make
```

Running the tests:
```shell
$ make test
```
(Refer [here](test/README.md) for more information on testing Rafka).

List all available commands:
```shell
$ make list
```










License
-------------------------------------------------------------------------------
Rafka is licensed under MIT. See [LICENSE](LICENSE).

