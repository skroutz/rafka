======================
 Server Shutdown Flow
======================

This document details the sequence of operations executed when Rafka server receives a shutdown
signal. The steps executed by the `Server` are summarized in the following list:

#. Receives the shutdown request
#. Stops accepting new `Consumers`
#. Closes all `Clients` with an active `Consumer` instance
#. Waits for all `Consumers` to terminate
#. Stops accepting new `Clients`
#. Closes `Clients` with an active `Producer` instance
#. Terminates

In the above rules set, there's a (rare) case which affects the described flow. Rafka currently
allows a single `Client` instance to maintain both a `Consumer` as well as a `Producer` instance.
When such `Client` is requested to close (3), its associated `Producer` will close too, without
waiting for all `Consumers` to terminate.

Furthermore, we should clarify that as soon as we're waiting for active `Consumers` to terminate,
new `Producers` can be registered. Based on Rafka server's load, a `Consumer` terminate operation
may last longer than expected; in that case we don't want to block the `Producers`, and so we allow
new `Producer` connections to be created.

.. vim: set textwidth=99 :
.. Local Variables:
.. mode: rst
.. fill-column: 99
.. End:
