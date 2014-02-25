(** A grab-bag of performance-oriented, UDP-oriented network tools.  These provide some
    convenience, but they are more complex than basic applications require.

    Defaults are chosen for typical UDP applications.  Buffering is via [Iobuf]
    conventions, where a typical packet-handling loop iteration is
    read-[flip_lo]-process-[reset].

    While these functions are oriented toward UDP, they work with any files that satisfy
    [Fd.supports_nonblock].

    For zero-copy [Bigstring.t] transfers, we must ensure no buffering between the receive
    loop and caller.  So, an interface like [Tcp.connect], with something like
    [(Bigstring.t * Socket.Address.Inet.t) Pipe.Reader.t], won't work.

    Instead, we use synchronous callbacks. *)

open Core.Std
open Import

type write_buffer = (read_write, Iobuf.seek) Iobuf.t

(** The default buffer capacity for UDP-oriented buffers is 1472, determined as the
    typical Ethernet MTU (1500 octets) less the typical UDP header length (28).  Using
    buffers of this size, one avoids accidentally creating messages that will be dropped
    on send because they exceed the MTU, and can receive the largest corresponding UDP
    message.

    While this number is merely typical and not guaranteed to work in all cases, defining
    it in one place makes it easy to share and change.  For example, another MTU in common
    use is 9000 for Jumbo frames, so the value of [default_capacity] might change to 8972
    in the future. *)
val default_capacity : int

(** A typical receive loop calls [before] before calling its callback to prepare a packet
    buffer for reading and [after] afterwards to prepare for writing (for the next
    iteration).

    One can specify [~before:ignore] or [~after:ignore] to disable the default action, as
    when doing buffer management in the callback.  One can also specify an action, such as
    [~after:Iobuf.compact] for use with [read_loop] on a connection-oriented socket or
    file.  It's often convenient to use the same interface for UDP, TCP, and file variants
    of the same protocol.

    [stop] terminates a typical loop as soon as possible, when it becomes determined.

    [max_ready] limits the number of receive loop iterations within an [Fd.every_ready_to]
    iteration, to prevent starvation of other Async jobs. *)
module Config : sig
  type t =
    { capacity : int
    ; init : write_buffer
    ; before : write_buffer -> unit
    ; after : write_buffer -> unit
    ; stop : unit Deferred.t
    ; max_ready : int
    } with fields

  val create
    :  ?capacity:int                    (** default is [default_capacity] *)
    -> ?init:write_buffer               (** default is [Iobuf.create ~len:capacity] *)
    -> ?before:(write_buffer -> unit)   (** default is [Iobuf.flip_lo] *)
    -> ?after:(write_buffer -> unit)    (** default is [Iobuf.reset] *)
    -> ?stop:(unit Deferred.t)          (** default is [Deferred.never] *)
    -> ?max_ready:int                   (** default is [12] *)
    -> unit
    -> t
end

(** [sendto_sync sock buf addr] does not try again if [sock] is not ready to write.
    Instead, it returns [`Not_ready] immediately.

    Short writes are distinguished by [buf] not being empty afterward.

    @raise Unix_error in the case of output errors.  See also
    {!Iobuf.sendto_nonblocking_no_sigpipe} and
    {!Bigstring.sendto_nonblocking_no_sigpipe}. *)
val sendto_sync
  :  unit
  -> (Fd.t -> (_, Iobuf.seek) Iobuf.t -> Socket.Address.Inet.t -> [ `Not_ready | `Ok ])
       Or_error.t
(** [sendto sock buf addr] retries if [sock] is not ready to write. *)
val sendto
  :  unit
  -> (Fd.t -> (_, Iobuf.seek) Iobuf.t -> Socket.Address.Inet.t -> unit Deferred.t)
       Or_error.t

val bind
  :  ?ifname:string
  -> Socket.Address.Inet.t
  -> ([ `Bound ], Socket.Address.Inet.t) Socket.t Deferred.t

val bind_any : unit -> ([ `Bound ], Socket.Address.Inet.t) Socket.t Deferred.t

(* Loops, including [recvfrom_loop], terminate normally when the socket is closed. *)
val recvfrom_loop
  :  ?config:Config.t
  -> Fd.t
  -> (write_buffer -> Socket.Address.Inet.t -> unit)
  -> unit Deferred.t
(** [recvfrom_loop_with_buffer_replacement callback] calls [callback] synchronously on
    each message received.  [callback] returns the packet buffer for subsequent
    iterations, so it can replace the initial packet buffer when necessary.  This enables
    immediate buffer reuse in the common case and fallback to allocation if we want to
    save the packet buffer for asynchronous processing. *)
val recvfrom_loop_with_buffer_replacement
  :  ?config:Config.t
  -> Fd.t
  -> (write_buffer -> Socket.Address.Inet.t -> write_buffer)
  -> write_buffer Deferred.t

val read_loop
  :  ?config:Config.t
  -> Fd.t
  -> (write_buffer -> unit)
  -> unit Deferred.t
val read_loop_with_buffer_replacement
  :  ?config:Config.t
  -> Fd.t
  -> (write_buffer -> write_buffer)
  -> write_buffer Deferred.t

(** [recvmmsg_loop ~socket callback] iteratively receives up to [max_count] packets at a
    time on [socket] and passes them to [callback].  Each packet is up to [capacity]
    bytes.  If [create_srcs], collect from-addresses there.

    [callback ?srcs bufs ~count] processes [count] packets synchronously.  [callback] may
    replace packet buffers in [bufs] and take ownership of the corresponding originals.
    [srcs] contains the corresponding source addresses of the packets in [bufs], if
    requested, and will similarly be reused when [callback] returns.

    [Config.init config] is used as a prototype for [bufs] and as one of the elements. *)
val recvmmsg_loop
  : (?config:Config.t                   (** default is [Config.create ()] *)
     -> ?create_srcs:bool               (** default is [false] *)
     -> ?max_count:int
     -> ?bufs:(write_buffer array)      (** supplies the packet buffers explicitly *)
     -> Fd.t
     -> (?srcs:(Core.Std.Unix.sockaddr array)
         -> write_buffer array
         -> count:int
         -> unit)                       (** may modify [bufs] or [srcs] *)
     -> unit Deferred.t)
      Or_error.t

(** [recvmmsg_no_sources_loop ~socket callback] is identical to [recvmmsg_loop], but can
    be used when sources are ignored to avoid some overhead incurred by optional
    arguments. *)
val recvmmsg_no_sources_loop
  : (?config:Config.t                   (** default is [Config.create ()] *)
     -> Fd.t
     -> ?max_count:int
     -> ?bufs:(write_buffer array)      (** supplies the packet buffers explicitly *)
     -> (write_buffer array
         -> count:int
         -> unit)                       (** may modify [bufs] *)
     -> unit Deferred.t)
      Or_error.t

val bind_to_interface_exn : (ifname:string -> Fd.t -> unit) Or_error.t
