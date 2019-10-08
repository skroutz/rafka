==================
 Rethinking Rafka
==================

.. contents:: :depth: 3

This is a design document detailing the plan for refactoring the internal structure of Rafka. Also,
the new simplified design will allow to provide the required native support for performance metrics
for the Rafka server.


Current state and shortcomings
==============================

The main rationale behind Rafka is to expose a subset of the Kafka API, i.e., the `Consumer` and
`Producer` API, by taking advantage of the Redis protocol and leveraging the already existing Redis
client libraries, mainly targeting languages that lack of a Kafka client library with proper
concurrency primitives. `However, the initial design decision for a simple and straightforward API
is not reflected in the internal structure of Rafka`.

Shortly, in order to implement the (simple) Rafka API, we internally maintain `five` primary
structs; the `Server`, `Client`, `Consumer`, `Producer`, and the `ConsumerManager`. Those structs
work interchangeably by operating on common data structures, which apparently translates to a lot
of extra lines of code for synchronizing and properly orchestrating the requests handling. Besides
the error-prone nature of that extra complexity, it proved to be quite difficult to follow the
execution flow and most important to isolate the interested parties in case of a bug or failure.

Furthermore, an important feature that Rafka currently lacks of, is the support of proper
monitoring primitives and metrics. There's no way to monitor the server's status, inspect the
Rafka-to-Kafka interaction, follow a request's life-cycle, and generally monitoring the server's
performance. There are a lot of incidents reported lately about lost messages on high-traffic
production setups and it is currently impossible to identify the root cause of those failures.


Background
==========

This section provides some extra information about Rafka internals, in order to facilitate the
reading of the rest of this design document.

Core components
---------------

The majority of Rafka functionality is internally handled by the following `5` major components. A
short description of each of those components, follows:

* ``Producer``, the interface used to send data streams to Kafka. It is allowed to produce to any
  Kafka topic using the same `Producer` instance.

* ``Consumer``, it allows reading data streams from Kafka. A `Consumer` is allowed to be subscribed
  to either one topic or a list of Kafka topics.

* ``ConsumerManager``, it maintains a mapping of **all** registered `Consumers` in the server. It
  uses this mapping to return the appropriate `Consumer` to the requested `Client`. There's a
  single `ConsumerManager` object per server and so it's scope is global.

* ``Client``, it is created upon a new connection to Rafka and can currently serve a `Producer`
  and/or one (or more) `Consumer` processes. It is associated with a client ID which apparently is
  unique across the server (this is ensured via the ``ClientByID`` server's field). Also note that
  each `Client` instance holds a reference to the global `ConsumerManager` object, too.

* ``Server``, it maintains a map with **all** registered `Clients`. It holds a reference to the
  global `ConsumerManager` object, just like the registered `Clients` do. It also takes over the
  Redis command handling.

Assumptions
-----------

As already mentioned, a `Client` can be mapped to a single `Producer` instance. At the same time,
the same `Client` object can be mapped to more than one `Consumer` instances, too. A new `Consumer`
instance will be created per subscribed Kafka topic (or topics). The subscribed topic(s) are also
included in the `ConsumerID`, which has the following format:


  ``<client-id>|<topic1(,topic2,..topicN)>``

Apparently, it is not allowed to create two consumers for the same topic for the same `Client`
instance. To better visualize this behavior, consider the following snippet using the common Redis
client:

.. code-block:: shell

  # Set the consumer group-id and name (client setname <group-id>:<name>)
  127.0.0.1:6380> client setname myapp:a-consumer
  "OK"
  # Create a consumer subscribed to topic test_topic_1
  #   consumerID: `myapp:a-consumer|test_topic_1`
  127.0.0.1:6380> blpop topics:test_topic_1 5
  (nil)
  (5.01s)
  # Create a consumer subscribed to topics test_topic_2 & test_topic_3
  #   consumerID: `myapp:a-consumer|test_topic_2,test_topic_3`
  127.0.0.1:6380> blpop topics:test_topic_2,test_topic_3 5
  (nil)
  (5.01s)
  # Try to create a consumer to the already subscribed topic test_topic_3
  127.0.0.1:6380> blpop topics:test_topic_3 5
  (error) CONS Topic test_topic_3 has another consumer

.. note::

  Note that when Rafka is used via the official Ruby client (aka `rafka-rb`_), it is not
  possible to create more than one consumers for the same connection (aka `Client`). The
  ``Rafka::Consumer`` API is mapped to a single Rafka `Client`, using a single `Consumer` instance,
  which is subscribed to a given list of topics. Using a tool like `redis-client` which allows
  operating on the same connection, is currently the only way to assign more than one consumers to
  a single `Client` instance.

.. _`rafka-rb`: https://github.com/skroutz/rafka-rb


Proposed changes
================

This document proposes a more simplified Rafka design; in order to make the source code more robust
and maintainable, we should decouple the primary Rafka structs from any extra logic as well as drop
the redundant synchronization functionality that we currently maintain. In short, the primary Rafka
structures should be abstracted further and become completely self-contained, if possible. This
refactoring will also translate to a lighter server implementation since we can move the Redis
command handling to the responsible modules. Finally, by limiting the project's scope and by
separating the modules' responsibilities, we ease the implementation of proper performance metrics'
support for the Rafka server.

The current proposal suggests splitting the implementation into 4 logical phases in order to ensure
a smoother transition to the new design. Finally, note that the external API and generally any
interaction with existing functionality won't be affected.

Redefine Rafka scope
--------------------

Rafka's main target is to expose a simple API to interact with Kafka. To keep it simply internally,
we could start with revisiting the `Consumer`-specific `Assumptions`_ we maintain for the `Client`
struct.  The question we should answer first is if there are any valid reasons to maintain this
`one-to-many` relationship between `Clients` and `Consumers`. `rafka-rb`_ already maintains an
`one-to-one` relationship between a `Client` and a `Consumer` and the only way to create a second
consumer for the same connection would be by using a tool which allow us to operate on the same
connection, like the `redis-client` for example. In short, we maintain a lot of extra logic for a
feature which is not likely to be used on production setups; it is impractical, never used, and we
should stop supporting it.

An immediate benefit of this modification is that the `Client` struct will be simplified
significantly, since the functionality related to this feature will be dropped completely, starting
with the ``consByTopic`` and ``consumers`` `Client` fields, as well as all the related parties. Of
course, for the same reason there's no way to support the creation of both a `Producer` and a
`Consumer` for the same `Client` object and this feature should be stopped supported, too.

Short story after; a `Client` would be mapped to either a single `Producer` instance or to a single
`Consumer` which can (apparently) be able to be subscribed to one or more topics.

Drop redundant functionality
----------------------------

Following the previous section's modifications, it allows us to get rid of the ``ConsumerManager``
module completely. Since the `Client` could only be mapped to a single `Consumer` at a time, the
`ConsumerManager` module becomes redundant. On the contrary, the `Client` module will become the
primary authoritative structure for handling its registered `Consumer` object, as it is
semantically expected to happen. It will take over the tasks to create, destroy, stop, or cancel a
registered `Consumer` without the extra overhead of the ``ConsumerManager`` logic.

Separation of concerns
----------------------

Moving a step forward, we can further abstract the internal API of the `Client` struct. What a
`Client` should actually know, is the connection and the ID that Rafka associates with it. The
implementation details of whether a `Client` corresponds to a `Producer` or a `Consumer` instance
should be moved away from this module by taking advantage of Go's interfaces and common Factory
method patterns. Separating the actions from the data will allow us to also make the ``MONITOR``
logic a first-class citizen similar to the `Producer` and `Consumer` ones.

Moreover, we could decouple the `Server` from the end-to-end handling of the supported Redis
commands. Ideally, each component (`Consumer`, `Producer`, `Monitor`) should be authoritative of
the Redis commands it can handle. For example, an incoming `BLPOP` request should not be handled by
the `Server` directly; instead, it should be forwarded to the interested party (aka the `Consumer`
in that case), which should properly process the command, create the write buffers, and respond
appropriately.

Performance metrics
-------------------

A production Rafka setup may handle a quite significant amount of traffic. Exporting metrics about
the server's state, is the only way to ensure that Rafka performs and operates smoothly. We could
expose metrics in various areas like:

- **Health metrics**, such as health and/or availability stats for the server
- **Server metrics**, such as clients connections, global requests count, etc
- **Client metrics**, such as producer metrics (number of messages received from a client,
  acknowledged, flushed to Kafka, etc), consumer-specific stats, etc

.. TODO: Implementation details of the current feature and enhanced metrics' list


Future enhancements
===================

Implementing **Error handling** primitives is a challenging task; Rafka currently lacks of proper
error handling techniques to handle and respond to such erroneous conditions. Decoupling the custom
error logic to a separate module, and creating proper error codes and handlers, will help in
maintaining the normal program's flow. We could probably combine such feature with the new `Error
Wrapping`_ feature which was introduced on `Go v1.13`.

.. TODO: More details about Error Handling in Rafka

.. _`Error Wrapping`: https://golang.org/doc/go1.13#error_wrapping


.. vim: set textwidth=99 :
.. Local Variables:
.. mode: rst
.. fill-column: 99
.. End:
