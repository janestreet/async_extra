open Core.Std
open Import

(** [Tcp] supports connection to [inet] sockets and [unix] sockets.  These are two
    different types.  We use ['a where_to_connect] to specify a socket to connect to,
    where the ['a] identifies the type of socket. *)
type 'a where_to_connect constraint 'a = [< Socket.Address.t ]

val to_host_and_port
  :  ?via_local_interface : Unix.Inet_addr.t  (** default is chosen by OS *)
  -> string
  -> int
  -> Socket.Address.Inet.t where_to_connect

val to_inet_address
  :  ?via_local_interface : Unix.Inet_addr.t  (** default is chosen by OS *)
  -> Socket.Address.Inet.t
  -> Socket.Address.Inet.t where_to_connect

val to_file          : string                -> Socket.Address.Unix.t where_to_connect
val to_unix_address  : Socket.Address.Unix.t -> Socket.Address.Unix.t where_to_connect

type 'a with_connect_options
  =  ?buffer_age_limit   : [ `At_most of Time.Span.t | `Unlimited ]
  -> ?interrupt          : unit Deferred.t
  -> ?reader_buffer_size : int
  -> ?timeout            : Time.Span.t
  -> 'a

(** [with_connection ~host ~port f] looks up host from a string (using DNS as needed),
    connects, then calls [f], passing the connected socket and a reader and writer for it.
    When the deferred returned by [f] is determined, or any exception is thrown, the
    socket, reader and writer are closed.  The return deferred is fulfilled after f has
    finished processing and the file descriptor for the socket is closed.  If [interrupt]
    is supplied then the connection attempt will be aborted if interrupt is fulfilled
    before the connection has been established.  Similarly, all connection attempts have a
    timeout (default 30s), that can be overridden with [timeout].

    It is fine for [f] to ignore the supplied socket and just use the reader and writer.
    The socket is there to make it convenient to call [Socket] functions. *)
val with_connection
  : ( 'addr where_to_connect
      -> (([ `Active ], 'addr) Socket.t -> Reader.t -> Writer.t -> 'a Deferred.t)
      -> 'a Deferred.t
    ) with_connect_options

(** [connect_sock ~host ~port] opens a TCP connection to the specified hostname
    and port, returning the socket.

    Any errors in the connection will be reported to the monitor that was current
    when connect was called. *)
val connect_sock : 'addr where_to_connect -> ([ `Active ], 'addr) Socket.t Deferred.t


(** [connect ~host ~port] is a convenience wrapper around [connect_sock] that returns the
    socket, and a reader and writer for the socket.  The reader and writer share a file
    descriptor, and so closing one will affect the other.  In particular, closing the
    reader before closing the writer will cause the writer to subsequently raise an
    exception when it attempts to flush internally-buffered bytes to the OS, due to a
    closed fd.  You should close the [Writer] first to avoid this problem.

    If possible, use [with_connection], which automatically handles closing.

    It is fine to ignore the returned socket and just use the reader and writer.  The
    socket is there to make it convenient to call [Socket] functions. *)
val connect
  : ( 'addr where_to_connect
      -> (([ `Active ], 'addr) Socket.t * Reader.t * Writer.t) Deferred.t
    ) with_connect_options

(** A [Where_to_listen] describes the socket that a tcp server should listen on. *)
module Where_to_listen : sig
  type ('address, 'listening_on) t constraint 'address = [< Socket.Address.t ]
  with sexp_of

  type inet = (Socket.Address.Inet.t, int   ) t with sexp_of
  type unix = (Socket.Address.Unix.t, string) t with sexp_of

  val create
    :  socket_type  : 'address Socket.Type.t
    -> address      : 'address
    -> listening_on : ('address -> 'listening_on)
    -> ('address, 'listening_on) t
end

val on_port              : int ->    Where_to_listen.inet
val on_port_chosen_by_os :           Where_to_listen.inet
val on_file              : string -> Where_to_listen.unix

(** A [Server.t] represents a TCP server listening on a socket. *)
module Server : sig

  type ('address, 'listening_on) t constraint 'address = [< Socket.Address.t ]

  type inet = (Socket.Address.Inet.t, int   ) t
  type unix = (Socket.Address.Unix.t, string) t

  val invariant : (_, _) t -> unit

  val listening_on : (_, 'listening_on) t -> 'listening_on

  (** [close t] starts closing the listening socket, and returns a deferred that becomes
      determined after [Fd.close_finished fd] on the socket's fd.  It is guaranteed that
      [t]'s client handler will never be called after [close t].  It is ok to call [close]
      multiple times on the same [t]; calls subsequent to the initial call will have no
      effect, but will return the same deferred as the original call.

      [close_finished] becomes determined after [Fd.close_finished fd] on the socket's fd,
      i.e. the same deferred that [close] returns.  [close_finished] differs from [close]
      in that it does not have the side effect of initiating a close.

      [is_closed t] returns [true] iff [close t] has been called. *)
  val close          : (_, _) t -> unit Deferred.t
  val close_finished : (_, _) t -> unit Deferred.t
  val is_closed      : (_, _) t -> bool

  (** [create where_to_listen handler] starts a server listening to a socket as specified
      by [where_to_listen].  It returns a server once the socket is ready to accept
      connections.  The server calls [handler (address, reader, writer)] for each client
      that connects.  If the deferred returned by [handler] is ever determined, or
      [handler] raises an exception, then [reader] and [writer] are closed.

      [max_pending_connections] is the maximum number of clients that can have a
      connection pending, as with [Unix.Socket.listen].  Additional connections will be
      rejected.

      [max_connections] is the maximum number of clients that can be connected
      simultaneously.  The server will not call [accept] unless the number of clients is
      less than [max_connections], although of course potential clients can have a
      connection pending.

      [buffer_age_limit] passes on to the underlying writer option of the same name.

      [on_handler_error] determines what happens if the handler throws an exception.  The
      default is [`Raise].  If an exception is raised by on_handler_error (either
      explicitely via `Raise, or in the closure passed to `Call) no further connections
      will be accepted.

      The server will stop accepting and close the listening socket when an error handler
      raises (either via [`Raise] or [`Call f] where [f] raises), or if [close] is
      called. *)
  val create
    :  ?max_connections         : int
    -> ?max_pending_connections : int
    -> ?buffer_age_limit        : Writer.buffer_age_limit
    -> ?on_handler_error        : [ `Raise
                                  | `Ignore
                                  | `Call of ('address -> exn -> unit)
                                  ]
    -> ('address, 'listening_on) Where_to_listen.t
    -> ('address -> Reader.t -> Writer.t -> unit Deferred.t)
    -> ('address, 'listening_on) t Deferred.t

  (** [listening_socket t] accesses the listening socket, which should be used with care.
      An anticipated use is with {!Udp.bind_to_interface_exn}.  Accepting connections on
      the socket directly will circumvent [max_connections] and [on_handler_error],
      however, and is not recommended. *)
  val listening_socket : ('address, _) t -> ([ `Passive ], 'address) Socket.t
end
