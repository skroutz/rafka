This directory contains Rafka's end-to-end tests. They test a full Rafka/Kafka
cluster using the [Rufka](https://github.com/skroutz/rufka) driver.

Dependencies:

* Ruby & bundler
* A Kafka cluster. [kafka-cluster-testbed](https://github.com/skroutz/kafka-cluster-testbed) provides a Dockerized setup

## Setup

Run `bundle` in this directory to ensure test dependencies are met.

## Usage

The easiest way to run the tests is by using Docker and
[kafka-cluster-testbed](https://github.com/skroutz/kafka-cluster-testbed):

First, start the Kafka cluster from kafka-cluster-testbed:

```shell
kafka-cluster-testbed/ $ docker-compose up
```

Then start the Rafka container:
```shell
$ docker-compose up
```

You can now run the tests:
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