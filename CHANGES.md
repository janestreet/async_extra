## 111.13.00

- For `Typed_tcp.create`, added a `Client_id.t` argument to the `auth`
  callback.

## 111.11.00

- Made `Log` more fair with respect to other Async jobs, by working on
  fixed-length groups of incoming log messages.

  Previously, `Log` had processed everything available.  The change
  gives other Async jobs more of a chance to run.

## 111.08.00

- Added `Log.Message.add_tags`, which extends a message with a list of
  key-value pairs.

        val add_tags : t -> (string * string) list -> t

## 111.06.00

- Added `?on_wouldblock:(unit -> unit)` callback to
  `Udp.recvmmsg_loop` and `recvmmsg_no_sources_loop`.
- For functions that create `Rpc` connections, added optional
  arguments: `?max_message_size:int` and
  `?handshake_timeout:Time.Span.t`.

  These arguments were already available to `Connection.create`, but
  are now uniformly available to all functions that create
  connections.

## 111.03.00

- Add `?max_connections:int` argument to `Rpc.Connection.serve`.

    `max_connections` is passed to `Tcp.Server.create`, and limits the
    number of connections that an Rpc server will accept.

- Improved `Log.Rotation`:

    - Made `Log.Rotation.t` abstract; use `create` rather than an
      explicit record.
    - Added a `` `Dated`` `naming_scheme`.
    - Add `Log.Rotation.default`, for getting a sensible default
      rotation scheme.
    - Added an optional (but discouraged) option to symlink the latest
      log file.
    - Every log rotation scheme has an associated `Time.Zone.t`.
    - Changed the internal representation of `Log.Rotation.t`, but
      `t_of_sexp` is backwards compatible, so existing config files will
      continue to work.

- Changed `Udp.bind_any` to use `Socket.bind ~reuseaddr:false`, to
  ensure a unique port.
- Added `Tcp.Server.listening_socket`, which returns the socket the
  server is listening on.

    Changed `Tcp.Server` so that if the listening socket is closed, the
    server is closed.

- Added to `Udp.Config.t` a `max_ready : int` field to prevent UDP
  receive loops from starving other async jobs.
- Improved `File_tail` to cut the number of `fstat` calls in half.

    `File_tail` uses a stat loop to monitor a file and continue reading
    it as it grows.  We had made two `fstat` invocations per loop
    iteration, using `Async.Std.Unix.with_file` which constructs an
    `Fd.t` and therefore does it own `fstat`.  Switching to
    `Core.Std.Unix.with_file` with `In_thread.run` eliminated the extra
    `fstat`.

## 110.01.00

- Added `Cpu_usage.Sampler` for directly sampling CPU usage.
- Fixed `Log.rotate` to never raise.
- Fixed two bugs in `Log` rotation.
  * Log rotation had used the wrong date when checking whether it
    should rotate.
  * Made `Rotation.keep = \`At_least` delete the oldest, rather than
    the newest, logs.

## 109.60.00

- Replaced `Tcp_file.serve`'s `~port:int` argument with
  `Tcp.Where_to_listen.inet`.

## 109.58.00

- Changed `Cpu_usage` to use `Core.Percent` instead of `float` where
  appropriate.
- Made `Bus.unsubscribe` check that the subscriber is subscribed to
  the given bus.
- Made `Log.t` support `with sexp_of`.
- Fixed `Tcp.on_port 0` to return the port actually being listened on,
  like `Tcp.on_port_chosen_by_os`.

  Previously, a serverlistening on `Tcp.on_port 0` would have its
  `Tcp.Server.listening_on` as `0`, which of course is not the port
  the server is listening on.

## 109.55.00

- Added `Udp.recvmmsg_no_sources_loop`, a specialization of
  `recvmmsg_loop` for improved performance.

  This improvement was driven by profiling at high message rates.

## 109.53.00

- Added module `Bus`, which is an intraprocess "broadcast"
  communication mechanism.
- Added `Tcp.to_inet_address` and `to_unix_address`.
- Added `Tcp.to_socket` which creates a `Tcp.where_to_connect` from a
  `Socket.Address.Inet.t`.
- Module `Weak_hashtbl` is now implemented as a wrapper around
  `Core.Weak_hashtbl`.

  No intended change in behavior.

## 109.52.00

- Added module `Cpu_usage`, which publishes CPU-usage statistics for
  the running process.
- Fixed `Sequencer_table.enqueue` so that there is no deferred between
  finding the state and calling the user function.

## 109.47.00

- Added `with sexp` to `Log.Output.machine_readable_format` and `format`.

## 109.45.00

- Added `?abort:unit Deferred.t` argument to
  `Lock_file.waiting_create`, `Lock_file.Nfs.waiting_create` and
  `critical_section`.

## 109.44.00

- Fixed a time-based race condition in `Log` rotation.

## 109.42.00

- Fixed `Log.Blocking` so that when async is running it writes the message in syslog before failing with an exception.

## 109.40.00

- Added to `Udp.Config` the ability to stop early, via `stop : unit Deferred.t`.

## 109.38.00

- In `Rpc`, exposed accessors for binary protocol values.

  For example, this allows one to write a wrapper for `Pipe_rpc` that
  allows for the easy re cording and replaying of values the come over
  the pipe.

## 109.35.00

- Added module `Async.Udp`, aimed at high-performance UDP
  applications.
- Added module `Lock_file.Nfs`, which wraps the functions in
  `Core.Std.Lock_file.Nfs`.

## 109.33.00

- Change `Log.Global` to by default send all output, including ``Info`,
  to `stderr`.

  Replaced `Log.Output.screen` with `Log.Output.stderr`.  There is now
  also and `Log.Output.stdout`.

## 109.32.00

- Added `Dynamic_port_writer`.

  `Dynamic_port_writer` solves the problem of communicating a
  dynamically selected tcp port from a child process to its parent.

## 109.28.00

- Fixed an error message in `Versioned_rpc` that was swapping which
  versions were supported by the caller and the callee.

## 109.27.00

- Added function `Versioned_typed_tcp.Client.shutdown`.
- Added new module `Sequencer_table`, which is a table of
  `Throttle.Sequencer`'s indexed by keys.

## 109.24.00

- Made the `Caller_converts` interface in `Versioned_rpc` use the
  `Connection_with_menu` idea introduced in `Both_convert`.

## 109.19.00

- Added function `Versioned_typed_tcp.Client.flushed : t ->
  [ `Flushed | `Pending of Time.t Deferred.t ]`.

  This exposes whether the underlying `Writer.t` has been flushed.

## 109.17.00

- Added an option to `Async.Log.Rotation` to include the date in
  logfile names.

  This is mostly for archiving purposes.
- Made `Versioned_rpc.Callee_converts.Pipe_rpc.implement_multi` agree
  with `Rpc.Pipe_rpc.implement` on the type of pipe rpc
  implementations.
- Improved the performance of `Versioned_typed_tcp`.

  Avoided creating deferreds while reading the incoming messages.

## 109.15.00

- In `Rpc.client` and `Rpc.with_client`, allowed the client to
  implement the rpcs.

  Added a new optional argument: `?implementations:_ Client_implementations.t`.
- Added new module `Versioned_rpc.Both_convert` to allow the caller
  and callee to independently upgrade to a new rpc.

  This is a new flavor of `Versioned_rpc` in which both sides do some
  type coercions.

## 109.12.00

- Made explicit the equivalence between type `Async.Command.t` and type `Core.Command.t`.

## 109.11.00

- Exposed a `version` function in `Pipe_rpc` and `State_rpc`.

## 109.10.00

- Fixed a race condition in `Pipe_rpc` and `State_rpc`.  This race
  could cause an exception to be raised on connection closing.

## 109.08.00

- Added module `Async.Command`
  This is `Core.Command` with additional async functions.  In particular
  it contains a function `async_basic` that is exactly the same as
  `Core.Command.basic`, except that the function it wraps returns
  `unit Deferred.t`, instead of `unit`.  `async_basic` will also start the
  async scheduler before the wrapped function is run, and will stop the
  scheduler when the wrapped function returns.

