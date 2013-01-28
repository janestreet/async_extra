(** Infrastructure code for managing RPCs which evolve over time to use
    different types at different versions *)
(**
  This module contains infrastructure code for managing RPCs which evolve
  over time to use different types at different versions.  Two scenarios
  are supported
    {ul
      {li The {i caller} is responsible for managing versions and
          dispatches to callees that are written in a version-oblivious
          way.

          The proto-typical example of this scenario is a commander that
          needs to call out to many assistants for that same system
          (e.g. friend commander calling friend assistants).  In this
          scenario, the assistants each implement a single version of
          the rpc and the commander has to take this into account.
      }
      {li The {i callee} is responsible for managing versions and
          is called by

          The proto-typical example of this scenario is an assistant
          from one system calling out the commander of another system
          (e.g. jentry assistants querying friend commander for
          symbology).  In this scenario, the assistants each know a
          single version of the rpc to call and the commander has to
          implement them all.
      }
    }

  In each scenario, it is desirable that the party responsible for
  managing versions be coded largely in terms of a single "master"
  version of the types involved, with all necessary type conversions
  relegated to a single module.  [Versioned_rpc] is intended for
  implementing such a module.

  Type coercions into and out of the model go in the directions indicated
  by the following diagram:
  {v

       Caller converts                 Callee converts
       ===============                 ===============

           caller                        callee
           |       callee                |      callee
           |       |       caller        |      |       callee
           |       |       |             |      |       |
        ,-->-- Q1 --> R1 -->-.      Q1 -->-.    |    ,-->-- R1
       /                      \             \   |   /
      Q --->-- Q2 --> R2 -->-- R    Q2 -->-- Q --> R --->-- R2
       \                      /             /       \
        `-->-- Q3 --> R3 -->-'      Q3 -->-'         `-->-- R3
  }
*)

open Core.Std
open Import

open Rpc

module Caller_converts : sig

  module Rpc : sig

    (* signature for rpc dispatchers *)
    module type S = sig
      type query
      type response

      (** multi-version dispatch *)
      val dispatch_multi :
        version:int -> Connection.t -> query -> (response, exn) Result.t Deferred.t

      (** all versions supported by [dispatch_multi].
          (useful for computing which old versions may be pruned) *)
      val versions : unit -> Int.Set.t
    end

    (**
      Given a model of the types involved in a family of RPCs, this functor
      provides a single RPC versioned dispatch function [dispatch_multi]
      in terms of that model and a mechanism for registering the individual
      versions that [dispatch_multi] knows about.  Registration requires
      knowing how to get into and out of the model.
      {v
          ,-->-- Q1 --> R1 -->-.
          /                      \
        Q --->-- Q2 --> R2 -->-- R
          \                      /
          `-->-- Q3 --> R3 -->-'
      }
    *)
    module Make (Model : sig
      val name : string  (* the name of the Rpc's being unified in the model *)
      type query
      type response
    end) : sig

      (** add a new version to the set of versions available via [dispatch_multi]. *)
      module Register (Version_i : sig
        val version : int
        type query with bin_io
        type response with bin_io
        val query_of_model : Model.query -> query
        val model_of_response : response -> Model.response
      end) : sig
        val rpc : (Version_i.query, Version_i.response) Rpc.t
      end

      include S with
        type query := Model.query and
        type response := Model.response
    end

  end

  module Pipe_rpc : sig

    (* signature for dispatchers *)
    module type S = sig
      type query
      type response
      type error

      (** multi-version dispatch *)
      val dispatch_multi :
        version:int
        -> Connection.t
        -> query
        -> ( (response Or_error.t Pipe.Reader.t * Pipe_rpc.Id.t, error) Result.t
           , exn
           ) Result.t Deferred.t

      (** all versions supported by [dispatch_multi].
          (useful for computing which old versions may be pruned) *)
      val versions : unit -> Int.Set.t
    end

    (**
      Given a model of the types involved in a family of Pipe_RPCs,
      this functor provides a single Pipe_RPC versioned dispatch
      function [dispatch_multi] in terms of that model and a mechanism
      for registering the individual versions that [dispatch_multi]
      knows about.  Registration requires knowing how to get into and
      out of the model.
      {v
          ,-->-- Q1 --> R1 -->-.    E1 -->-.
         /                      \           \
        Q --->-- Q2 --> R2 -->-- R  E2 -->-- E
         \                      /           /
          `-->-- Q3 --> R3 -->-'    E3 -->-'
      }
    *)
    module Make (Model : sig
      val name : string
      type query
      type response
      type error
    end) : sig

      (** add a new version to the set of versions available via [dispatch_multi]. *)
      module Register (Version_i : sig
        val version : int
        type query with bin_io
        type response with bin_io
        type error with bin_io
        val query_of_model : Model.query -> query
        val model_of_response : response -> Model.response
        val model_of_error : error -> Model.error
      end) : sig
        val rpc : (Version_i.query, Version_i.response, Version_i.error) Pipe_rpc.t
      end

      include S with
        type query    := Model.query and
        type response := Model.response and
        type error    := Model.error
    end

  end

end

module Callee_converts : sig

  module type S = sig
    type query
    type response

    (** implement multiple versions at once *)
    val implement_multi :
      ?log_not_previously_seen_version:(name:string -> int -> unit)
      -> ('state -> query -> response Deferred.t)
      -> 'state Implementation.t list

    (** all versions implemented by [implement_multi]
        (useful for computing which old versions may be pruned) *)
    val versions : unit -> Int.Set.t
  end

  (**
    Given a model of the types involved in a family of RPCs, this
    functor provides a single multi-version implementation function
    [implement_multi] in terms of that model and a mechanism for
    registering the individual versions that [implement_multi] knows
    about.  Registration requires knowing how to get into and out of
    the model.
    {v
       Q1 -->-.         ,-->-- R1
               \       /
       Q2 -->-- Q --> R --->-- R2
               /       \
       Q3 -->-'         `-->-- R3
    }
  *)
  module Make (Model : sig
    val name : string
    type query
    type response
  end) : sig

    (** add a new version to the set of versions implemented by [implement_multi]. *)
    module Register (Version_i : sig
      val version : int
      type query with bin_io
      type response with bin_io
      val model_of_query : query -> Model.query
      val response_of_model : Model.response -> response
    end) : sig
      val rpc : (Version_i.query, Version_i.response) Rpc.t
    end

    include S with
      type query := Model.query and
      type response := Model.response
  end

end

(** machinery for communicating names and versions of supported rpcs from
    within the rpc protocol itself.

    This is useful for dynamically determining the most appropriate version number
    to pass [dispatch_multi]. when using [Caller_converts]-style versioned rpcs *)
module Menu : sig

  type t (** a directory of supported rpc names and versions. *)

  (** [add impls] extends a list of rpc implementations with an additional
      rpc implementation for providing a [Menu.t] when one is requested
      via [Menu.request]. *)
  val add : 's Implementation.t list -> 's Implementation.t list

  (** request an rpc version menu from an rpc connection *)
  val request : Connection.t -> (t, exn) Result.t Deferred.t

  (** find what versions of a particular rpc are supported *)
  val supported_versions : t -> rpc_name:string -> Int.Set.t

end

