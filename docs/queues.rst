Queues Available for Colmena
============================

Colmena supports multiple backends for queues that pass data between Thinker and Task Server
(see `Design <design.html#communication>`_)

Python Pipes
------------

:class:`~colmena.queue.python.PipeQueues` uses `Python Pipes <https://docs.python.org/3/library/multiprocessing.html#pipes-and-queues>`_ transmit data
between two Python processes.

**Advantages**: Simple setup (no configuration or other services required)

**Disadvantages**:

- Small message sizes (<32 MiB)
- Thinker and Task Server must be on same system
- Only one Thinker and Task server are allowed

Redis
-----

:class:`~colmena.queue.redis.RedisQueues` uses `Redis <https://redis.io/>`_, a high-performance in-memory data store.

**Advantages**:

- Support moderate message sizes (<512 MiB)
- Thinker and Task Server can run on different systems
- Applications can use multiple Thinkers and Task Servers
- Redis server can also serve as a backend for ProxyStore

**Disadvantages**;

- Redis must run as a second service
- Open ports or SSH tunnels may be required if Redis on separate host from Task Server/Thinker
