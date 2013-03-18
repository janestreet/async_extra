
open Core.Std
open Import
open Rpc_intf

(** A library for building asynchronous RPC-style protocols.

    The approach here is to have a separate representation of the server-side
    implementation of an RPC (An [Implementation.t]) and the interface that it exports
    (either an [Rpc.t], a [State_rpc.t] or a [Pipe_rpc.t], but we'll refer to them
    generically as RPC interfaces).  A server builds the [Implementation.t] out of an RPC
    interface and a function for implementing the RPC, while the client dispatches a
    request using the same RPC interface.

    The [Implementation.t] hides the type of the query and the response, whereas the
    [Rpc.t] is polymorphic in the query and response type.  This allows you to build a
    [Implementations.t] out of a list of [Implementation.t]s.

    Each RPC also comes with a version number.  This is meant to allow support of multiple
    different versions of what is essentially the same RPC.  You can think of it as an
    extension to the name of the RPC, and in fact, each RPC is uniquely identified by its
    (name, version) pair.  RPCs with the same name but different versions should implement
    similar functionality. *)

module Implementation : sig
  (** A ['connection_state t] is something which knows how to respond to one query, given
      a ['connection_state].  That is, you can create a ['connection_state t] by providing
      a function which takes a query *and* a ['connection_state] and provides a response.

      The reason for this is that rpcs often do something like look something up in a
      master structure.  This way, [Implementation.t]'s can be created without having the
      master structure in your hands. *)
  type 'connection_state t

  module Description : sig
    type t = { name : string; version : int; } with sexp
  end

  val description : _ t -> Description.t

  (** We may want to use an ['a t] implementation (perhaps provided by someone else) in a
      ['b t] context. We can do this as long as we can map our state into the state
      expected by the original implementer. *)
  val lift : 'a t -> f:('b -> 'a) -> 'b t
end

module Implementations : sig
  (** A ['connection_state Implementations.t] is something which knows how to respond to
      many different queries. It is conceptually a package of ['connection_state
      Implementation.t]'s. *)
  type 'connection_state t

  (** a server that can handle no queries *)
  val null : unit -> 'connection_state t

  (** [create ~implementations ~on_unknown_rpc] creates a server
      capable of responding to the rpc's implemented in the implementation list. *)
  val create
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[
    | `Raise
    | `Ignore
    (** [rpc_tag] and [version] are the name and version of the unknown rpc *)
    | `Call of (rpc_tag:string -> version:int -> unit)
    ]
    -> ( 'connection_state t
       , [`Duplicate_implementations of Implementation.Description.t list]
       ) Result.t

  val create_exn
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[
    | `Raise
    | `Ignore
    | `Call of (rpc_tag:string -> version:int -> unit)
    ]
    -> 'connection_state t
end

module type Connection = Connection with module Implementations := Implementations

module Connection : Connection

module Rpc : sig
  type ('query, 'response) t

  val create
    :  name:string
    -> version:int
    -> bin_query    : 'query    Bin_prot.Type_class.t
    -> bin_response : 'response Bin_prot.Type_class.t
    -> ('query, 'response) t

  (** the same values as were passed to create. *)
  val name    : (_, _) t -> string
  val version : (_, _) t -> int

  val implement
    :  ('query, 'response) t
    -> ('connection_state
        -> 'query
        -> 'response Deferred.t)
    -> 'connection_state Implementation.t

  val dispatch
    :  ('query, 'response) t
    -> Connection.t
    -> 'query
    -> 'response Or_error.t Deferred.t

  val dispatch_exn
    :  ('query, 'response) t
    -> Connection.t
    -> 'query
    -> 'response Deferred.t
end

module Pipe_rpc : sig
  type ('query, 'response, 'error) t

  module Id : sig type t end

  val create
    :  name:string
    -> version:int
    -> bin_query    : 'query    Bin_prot.Type_class.t
    -> bin_response : 'response Bin_prot.Type_class.t
    -> bin_error    : 'error    Bin_prot.Type_class.t
    -> ('query, 'response, 'error) t

  val implement
    :  ('query, 'response, 'error) t
    -> ('connection_state
        -> 'query
        -> aborted:unit Deferred.t
        -> ('response Pipe.Reader.t, 'error) Result.t Deferred.t)
    -> 'connection_state Implementation.t

  (** This has [(..., 'error) Result.t] as its return type to represent the possibility of
      the call itself being somehow erroneous (but understood - the outer [Or_error.t]
      encompasses failures of that nature).  Note that this cannot be done simply by making
      ['response] a result type, since [('response Pipe.Reader.t, 'error) Result.t] is
      distinct from [('response, 'error) Result.t Pipe.Reader.t]. *)
  val dispatch
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Id.t, 'error) Result.t Or_error.t Deferred.t

  val dispatch_exn
    :  ('query, 'response, 'error) t
    -> Connection.t
    -> 'query
    -> ('response Pipe.Reader.t * Id.t) Deferred.t

  (** [abort rpc connection id] given an RPC and the id returned as part of a call to
      dispatch, abort requests that the other side of the connection stop sending
      updates. *)
  val abort : (_, _, _) t -> Connection.t -> Id.t -> unit

  val name : (_, _, _) t -> string
  val version : (_, _, _) t -> int
end

(** A state rpc is an easy way for two processes to synchronize a data structure by
    sending updates over the wire.  It's basically a pipe rpc that sends/receives an
    initial state of the data structure, and then updates, and applies the updates under
    the covers. *)
module State_rpc : sig
  type ('query, 'state, 'update, 'error) t

  module Id : sig type t end

  val create
    :  name:string
    -> version:int
    -> bin_query  : 'query  Bin_prot.Type_class.t
    -> bin_state  : 'state  Bin_prot.Type_class.t
    -> bin_update : 'update Bin_prot.Type_class.t
    -> bin_error  : 'error  Bin_prot.Type_class.t
    -> ('query, 'state, 'update, 'error) t

  val implement
    :  ('query, 'state, 'update, 'error) t
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
    -> ( 'state * ('state * 'update) Pipe.Reader.t * Id.t
       , 'error
       ) Result.t Or_error.t Deferred.t

  val abort : (_, _, _, _) t -> Connection.t -> Id.t -> unit

  val name : (_, _, _, _) t -> string
  val version : (_, _, _, _) t -> int
end
