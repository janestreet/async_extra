
open Core.Std
open Import

(* The reason for defining this module type explicitly is so that we can internally keep
   track of what is and isn't exposed. *)
module type Connection = sig
  module Implementations : sig
    type 'a t
  end

  type t


  (** Initiate an Rpc connection on the given reader/writer pair.  [server] should be the
      bag of implementations that the calling side implements; it defaults to
      [Implementations.null] (i.e., "I implement no RPCs"). *)
  val create
    :  ?implementations:'s Implementations.t
    -> connection_state:'s
    -> ?max_message_size:int
    -> Reader.t
    -> Writer.t
    -> (t, Exn.t) Result.t Deferred.t

  (* [close] starts closing the connection's reader and writer, and returns a deferred
     that becomes determined when their close completes.  It is ok to call [close]
     multiple times on the same [t]; calls subsequent to the initial call will have no
     effect, but will return the same deferred as the original call.

     [close_finished] becomes determined after the close of the connection's reader and
     writer completes, i.e. the same deferred that [close] returns.  [close_finished]
     differs from [close] in that it does not have the side effect of initiating a close.

     [is_closed t] returns [true] iff [close t] has been called. *)
  val close          : t -> unit Deferred.t
  val close_finished : t -> unit Deferred.t
  val is_closed      : t -> bool

  val bytes_to_write : t -> int

  (** [with_close] tries to create a [t] using the given reader and writer.  If a
      handshake error is the result, it calls [on_handshake_error], for which the default
      behavior is to raise an exception.  If no error results, [dispatch_queries] is
      called on [t].

      After [dispatch_queries] returns, if [server] is None, the [t] will be closed and
      the deferred returned by [dispatch_queries] wil be determined immediately.
      Otherwise, we'll wait until the other side closes the connection and then close [t]
      and determine the deferred returned by [dispatch_queries].

      When the deferred returned by [with_close] becomes determined, both [Reader.close]
      and [Writer.close] have finished. *)
  val with_close
    :  ?implementations:'s Implementations.t
    -> connection_state:'s
    -> Reader.t
    -> Writer.t
    -> dispatch_queries:(t -> 'a Deferred.t)
    -> on_handshake_error:[
    | `Raise
    | `Call of (Exn.t -> 'a Deferred.t)
    ]
    -> 'a Deferred.t

  (* Runs [with_close] but dispatches no queries. The implementations are required because
     this function doesn't let you dispatch any queries (i.e., act as a client), it would
     be pointless to call it if you didn't want to act as a server.*)
  val server_with_close
    :  Reader.t
    -> Writer.t
    -> implementations:'s Implementations.t
    -> connection_state:'s
    -> on_handshake_error:[
    | `Raise
    | `Ignore
    | `Call of (Exn.t -> unit Deferred.t)
    ]
    -> unit Deferred.t

  (** [serve implementations ~port ?on_handshake_error ()] starts a server with the given
      implementation on [port].  The optional auth function will be called on all incoming
      connections with the address info of the client and will disconnect the client
      immediately if it returns false.  This auth mechanism is generic and does nothing
      other than disconnect the client - any logging or record of the reasons is the
      responsibility of the auth function itself. *)
  val serve
    :  implementations:'s Implementations.t
    -> initial_connection_state:('address -> 's)
    -> where_to_listen:('address, 'listening_on) Tcp.Where_to_listen.t
    -> ?auth:('address -> bool)
    (** default is [`Ignore] *)
    -> ?on_handshake_error:[
      | `Raise
      | `Ignore
      | `Call of (Exn.t -> unit)
    ]
    -> unit
    -> ('address, 'listening_on) Tcp.Server.t Deferred.t

  module Client_implementations : sig
    type 's t = {
      connection_state : 's;
      implementations : 's Implementations.t;
    }

    val null : unit -> unit t
  end

  (** [client ~host ~port ()] connects to the server at ([host],[port]) and returns the
      connection or an Error if a connection could not be made.  It is the responsibility
      of the caller to eventually call close. *)
  val client
    :  host:string
    -> port:int
    -> ?implementations:_ Client_implementations.t
    -> unit
    -> (t, Exn.t) Result.t Deferred.t

  (** [with_client ~host ~port f] connects to the server at ([host],[port]) and runs f
      until an exception is thrown or until the returned Deferred is fulfilled. *)
  val with_client
    :  host:string
    -> port:int
    -> ?implementations:_ Client_implementations.t
    -> (t -> 'a Deferred.t)
    -> ('a, Exn.t) Result.t Deferred.t
end
