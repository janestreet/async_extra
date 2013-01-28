open Core.Std
open Import

(** A library for building asynchronous RPC-style protocols.

    The approach here is to have a separate representation of the server-side
    implementation of an RPC (An [Implementation.t]) and the interface that it exports
    (either an [Rpc.t], a [State_rpc.t] or a [Pipe_rpc.t], but we'll refer to them
    generically as RPC interfaces).  A server builds the [Implementation.t] out of an RPC
    interface and a function for implementing the RPC, while the client dispatches a
    request using the same RPC interface.

    The [Implementation.t] hides the type of the query and the response, whereas the
    [Rpc.t] is polymorphic in the query and response type.  This allows you to build a
    [Server.t] out of a list of [Implementation.t]s.

    Each RPC also comes with a version number.  This is meant to allow support of multiple
    different versions of what is essentially the same RPC.  You can think of it as an
    extension to the name of the RPC, and in fact, each RPC is uniquely identified by its
    (name, version) pair.  RPCs with the same name but different versions should implement
    similar functionality.
*)

module Implementation : sig
  (* A ['connection_state t] is something which knows how to respond to one query, given
     a ['connection_state].  That is, you can create a ['connection_state t] by providing
     a function which takes a query *and* a ['connection_state] and provides a response.

     The reason for this is that rpcs often do something like look something up in a
     master structure.  This way, [Implementation.t]'s can be created without having
     the master structure in your hands.
  *)
  type 'connection_state t

  module Description : sig
    type t = { name : string; version : int; }
    include Sexpable with type t := t
  end

  val description : _ t -> Description.t
end

module Server : sig
  (* A ['connection_state Server.t] is something which knows how to respond to many
     different queries. It is conceptually a package of
     ['connection_state Implementation.t]'s. *)
  type 'connection_state t

  (* a server that can handle no queries *)
  val null : unit -> 'connection_state t

  (** [create ~implementations ~on_unknown_rpc] creates a server
      capable of responding to the rpc's implemented in the implementation list. *)
  val create :
    implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[
    | `Raise
    | `Ignore
    | `Call of (rpc_tag:string -> version:int -> unit)
    ]
    -> ( 'connection_state t
       , [`Duplicate_implementations of Implementation.Description.t list]
       ) Result.t

  val create_exn :
    implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[
    | `Raise
    | `Ignore
    | `Call of (rpc_tag:string -> version:int -> unit)
    ]
    -> 'connection_state t
end

(* The reason for defining this module type explicitly is so that we can internally keep
   track of what is and isn't exposed. *)
module type Connection = sig
  type t


  (** Initiate an Rpc connection on the given reader/writer pair. [server] should be the
      bag of implementations that the calling side implements; it defaults to
      [Server.null] (i.e., "I implement no RPCs"). *)
  val create :
    ?server:'s Server.t
    -> connection_state:'s
    -> ?max_message_size:int
    -> Reader.t
    -> Writer.t
    -> (t, Exn.t) Result.t Deferred.t

  val close : t -> unit
  val closed : t -> unit Deferred.t
  val already_closed : t -> bool
  val bytes_to_write : t -> int


  (** [with_close] tries to create a [t] using the given reader and writer.  If a
      handshake error is the result, it calls [on_handshake_error], for which the default
      behavior is to raise an exception.  If no error results, [dispatch_queries] is
      called on [t].

      After [dispatch_queries] returns, if [server] is None, the [t] will be closed and
      the deferred returned by [dispatch_queries] wil be determined immediately.
      Otherwise, we'll wait until the other side closes the connection and then close [t]
      and determine the deferred returned by [dispatch_queries].  *)
  val with_close :
    ?server:'s Server.t
    -> connection_state:'s
    -> Reader.t
    -> Writer.t
    -> dispatch_queries:(t -> 'a Deferred.t)
    -> on_handshake_error:[
    | `Raise
    | `Call of (Exn.t -> 'a Deferred.t)
    ]
    -> 'a Deferred.t

  (* Runs [with_close] but dispatches no queries. The server is required because this
     function doesn't let you dispatch any queries (i.e., act as a client), it would be
     pointless to call it if you didn't want to act as a server.*)
  val server_with_close :
    Reader.t
    -> Writer.t
    -> server:'s Server.t
    -> connection_state:'s
    -> on_handshake_error:[
    | `Raise
    | `Ignore
    | `Call of (Exn.t -> unit Deferred.t)
    ]
    -> unit Deferred.t

  (** [serve server ~port ?on_handshake_error ()] starts a server with the given
      implementation on [port].  The optional auth function will be called on all incoming
      connections with the address info of the client and will disconnect the client
      immediately if it returns false.  This auth mechanism is generic and does nothing
      other than disconnect the client - any logging or record of the reasons is the
      responsibility of the auth function itself. *)
  val serve :
    server:'s Server.t
    -> initial_connection_state:(Socket.inet -> 's)
    -> port:int
    -> ?auth:(Socket.inet -> bool)
    -> ?on_handshake_error:[
      | `Raise
      | `Ignore
      | `Call of (Exn.t -> unit)
    ]
    -> unit
    -> unit Deferred.t

  (** [client ~host ~port] connects to the server at ([host],[port]) and returns the
      connection or an Error if a connection could not be made.  It is the responsibility
      of the caller to eventually call close. *)
  val client :
    host:string
    -> port:int
    -> (t, Exn.t) Result.t Deferred.t

  (** [with_client ~host ~port f] connects to the server at ([host],[port]) and runs f
      until an exception is thrown or until the returned Deferred is fulfilled. *)
  val with_client :
       host:string
    -> port:int
    -> (t -> 'a Deferred.t)
    -> ('a, Exn.t) Result.t Deferred.t
end
module Connection : Connection

module Rpc : sig
  type ('query,'response) t

  val create :
    name:string
    -> version:int
    -> bin_query    : 'query    Bin_prot.Type_class.t
    -> bin_response : 'response Bin_prot.Type_class.t
    -> ('query,'response) t

  (* the same values as were passed to create. *)
  val name    : (_, _) t -> string
  val version : (_, _) t -> int

  val implement :
    ('query,'response) t
    -> ('connection_state
        -> 'query
        -> 'response Deferred.t)
    -> 'connection_state Implementation.t

  val dispatch :
    ('query, 'response) t
    -> Connection.t
    -> 'query
    -> ('response, Exn.t) Result.t Deferred.t

  val dispatch_exn : ('query, 'response) t
    -> Connection.t
    -> 'query
    -> 'response Deferred.t
end

module Pipe_rpc : sig
  type ('query, 'response, 'error) t

  module Id : sig type t end

  val create :
    name:string
    -> version:int
    -> bin_query    : 'query    Bin_prot.Type_class.t
    -> bin_response : 'response Bin_prot.Type_class.t
    -> bin_error    : 'error    Bin_prot.Type_class.t
    -> ('query, 'response, 'error) t

  val implement :
    ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> aborted:unit Deferred.t
        -> ('response Pipe.Reader.t, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val dispatch :
    ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> (('response Pipe.Reader.t * Id.t, 'error) Result.t, Exn.t) Result.t Deferred.t

  val dispatch_exn :
    ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Id.t) Deferred.t

  (* [abort rpc connection id] given an RPC and the id returned as part of a call to
     dispatch, abort requests that the other side of the connection stop sending
     updates.
  *)
  val abort : (_, _, _) t -> Connection.t -> Id.t -> unit

  val name :(_,_,_) t -> string
end

(* A state rpc is an easy way for two processes to synchronize a data structure by sending
   updates over the wire.  It's basically a pipe rpc that sends/receives an initial state
   of the data structure, and then updates, and applies the updates under the covers.
*)
module State_rpc : sig
  type ('query, 'state, 'update, 'error) t

  module Id : sig type t end

  val create :
    name:string
    -> version:int
    -> bin_query  : 'query  Bin_prot.Type_class.t
    -> bin_state  : 'state  Bin_prot.Type_class.t
    -> bin_update : 'update Bin_prot.Type_class.t
    -> bin_error  : 'error  Bin_prot.Type_class.t
    -> ('query, 'state, 'update, 'error) t

  val implement :
    ('query, 'state, 'update, 'error) t
    -> ('connection_state
        -> 'query
        -> aborted:unit Deferred.t
        -> (('state * 'update Pipe.Reader.t), 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  val dispatch :
    ('query, 'state, 'update, 'error) t
    -> Connection.t
    -> 'query
    -> update:('state -> 'update -> 'state)
    -> (('state * ('state * 'update) Pipe.Reader.t * Id.t,
         'error) Result.t, Exn.t) Result.t Deferred.t

  val abort : (_,_,_,_) t -> Connection.t -> Id.t -> unit

  val name :(_,_,_,_) t -> string
end


