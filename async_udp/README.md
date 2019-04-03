# `Async_udp`

A grab-bag of performance-oriented, UDP-oriented network tools.
Formerly known as the module `Async.Udp`.

New code should consider instead using the `netkit` library, which has
a number of advantages.  The `netkit` abstractions allow the use of
sockets and the Async scheduler as one possible backend, but also
support alternate schedulers and zero-copy semantics.
High-performance code is moving away from the sockets api and
associated Unix I/O multiplexing schemes (epoll/poll/select).
