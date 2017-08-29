This directory contains Rafka's end-to-end tests. They test a full Rafka/Kafka
cluster using the [Rafka ruby driver](https://github.com/skroutz/rafka-rb).

Dependencies:

* Ruby & [bundler](http://bundler.io/)
* A Kafka cluster. [kafka-cluster-testbed](https://github.com/skroutz/kafka-cluster-testbed) provides a Dockerized setup and
  is the recommended way to test Rafka.

## Setup

Run `$ bundle` in this directory to ensure test dependencies are satisfied.

## Usage

The easiest way to run the tests is by using Docker and
[kafka-cluster-testbed](https://github.com/skroutz/kafka-cluster-testbed):

First, start the Kafka cluster from kafka-cluster-testbed:
```shell
kafka-cluster-testbed/ $ docker-compose up
```

Then start the Rafka container (inside this directory):
```shell
$ docker-compose up
```

(Note: Additionally, you can use `make spawn` from the root project directory
to spin up a rafka server without using docker-compose)

Finally, run the tests:
```shell
$ ./end-to-end
```

To run a specific test:
```shell
$ ./end-to-end --name <test-name>
```

Specifying the Rafka server to connect to (default is "localhost:6380"):
```shell
$ RAFKA=127.0.0.1:6381 ./end-to-end
```

## Updating dependencies
To update dependencies (eg. when a new librdkafka version is out) you have to
rebuild the docker image without using the cache:

```shell
$ docker-compose build --no-cache
```

It's good to do this once in a while since there may be new versions out.


