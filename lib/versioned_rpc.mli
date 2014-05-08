(** Infrastructure code for managing RPCs that evolve over time to use different types at
    different versions.

    Three scenarios are supported:

    {ul

     {li

     The {i caller} is responsible for managing versions and dispatches to callees that
     are written in a version-oblivious way.

     The proto-typical example of this scenario is a commander that needs to call out to
     many assistants for that same system.  In this scenario, the assistants each
     implement a single version of the rpc and the commander has to take this into
     account. }

     {li

     The {i callee} is responsible for managing versions and callers need not bother
     themselves with any versions.

     The proto-typical example of this scenario is an assistant from one system calling
     out the commander of another system.  In this scenario, the assistants each know a
     single version of the rpc to call and the commander has to implement them all. }

     {li

     Both {i caller} and {i callee} cooperate to decide which version to use, each one
     being able to use some subset of all possible versions.

     The proto-typical example of this scenario is when two systems developed
     independently with their rpc types defined in some shared library that has yet
     another independent rollout schedule.  In this case one may roll out a new rpc
     version (V) in the shared library (L) and then the caller and callee systems can each
     upgrade to the new version of L supporting version V at their own pace, with version
     V only being exercised once both caller and callee have upgraded.  }
    }

    In each scenario, it is desirable that the party responsible for managing versions be
    coded largely in terms of a single "master" version of the types involved, with all
    necessary type conversions relegated to a single module.  [Versioned_rpc] is intended
    for implementing such a module.

    Type coercions into and out of the model go in the directions indicated by the
    following diagram:

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
   v}
*)

open Core.Std
open Import

open Rpc

(* Over the network discovery of rpc names and versions supported by a callee.

   This is used by the [dispatch_multi] functions in [Caller_converts] and [Both_convert]
   to dynamically determine the most appropriate version to use. *)
module Menu : sig

  type t (** a directory of supported rpc names and versions. *)

  (** [add impls] extends a list of rpc implementations with an additional rpc
      implementation for providing a [Menu.t] when one is requested via [Menu.request]. *)
  val add : 's Implementation.t list -> 's Implementation.t list

  (** request an rpc version menu from an rpc connection *)
  val request : Connection.t -> t Or_error.t Deferred.t

  (** find what rpcs are supported *)
  val supported_rpcs : t -> Implementation.Description.t list

  (** find what versions of a particular rpc are supported *)
  val supported_versions : t -> rpc_name:string -> Int.Set.t

end

module Connection_with_menu : sig
  type t (** an rpc connection paired with the menu of rpcs one may call on it *)
  val create : Connection.t -> t Deferred.Or_error.t
  val connection : t -> Connection.t
  val menu : t -> Menu.t
end

module Caller_converts : sig

  module Rpc : sig

    (* signature for rpc dispatchers *)
    module type S = sig
      type query
      type response

      val deprecated_dispatch_multi
        : version:int -> Connection.t -> query -> response Or_error.t Deferred.t

      (** multi-version dispatch *)
      val dispatch_multi
        : Connection_with_menu.t -> query -> response Or_error.t Deferred.t

      (** All versions supported by [dispatch_multi].
          (useful for computing which old versions may be pruned) *)
      val versions : unit -> Int.Set.t
    end

    (** Given a model of the types involved in a family of RPCs, this functor provides a
        single RPC versioned dispatch function [dispatch_multi] in terms of that model and
        a mechanism for registering the individual versions that [dispatch_multi] knows
        about.  Registration requires knowing how to get into and out of the model.

       {v
           ,-->-- Q1 --> R1 -->-.
          /                      \
         Q --->-- Q2 --> R2 -->-- R
          \                      /
           `-->-- Q3 --> R3 -->-'
       v}
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

      (** multi-version dispatch

          The return type varies slightly from [Rpc.Pipe_rpc.dispatch] to make it clear
          that conversion of each individual element in the returned pipe may fail. *)

      val deprecated_dispatch_multi
        :  version:int
        -> Connection.t
        -> query
        -> ( response Or_error.t Pipe.Reader.t * Pipe_rpc.Id.t
           , error
           ) Result.t Or_error.t Deferred.t

      val dispatch_multi
        : Connection_with_menu.t
        -> query
        -> ( response Or_error.t Pipe.Reader.t * Pipe_rpc.Id.t
           , error
           ) Result.t Or_error.t Deferred.t

      (** All versions supported by [dispatch_multi].
          (useful for computing which old versions may be pruned) *)
      val versions : unit -> Int.Set.t
    end

    (** Given a model of the types involved in a family of Pipe_RPCs, this functor
        provides a single Pipe_RPC versioned dispatch function [dispatch_multi] in terms
        of that model and a mechanism for registering the individual versions that
        [dispatch_multi] knows about.  Registration requires knowing how to get into and
        out of the model.

        {v
            ,-->-- Q1 --> R1 -->-.    E1 -->-.
           /                      \           \
          Q --->-- Q2 --> R2 -->-- R  E2 -->-- E
           \                      /           /
            `-->-- Q3 --> R3 -->-'    E3 -->-'
        v}
    *)
    module Make (Model : sig
                   val name : string  (* the name of the Rpc's being unified in the model *)
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

  module Rpc : sig

    module type S = sig
      type query
      type response

      (** implement multiple versions at once *)
      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> query -> response Deferred.t)
        -> 'state Implementation.t list

      (** All versions implemented by [implement_multi].
          (useful for computing which old versions may be pruned) *)
      val versions : unit -> Int.Set.t
    end

    (** Given a model of the types involved in a family of RPCs, this functor provides a
        single multi-version implementation function [implement_multi] in terms of that
        model and a mechanism for registering the individual versions that
        [implement_multi] knows about.  Registration requires knowing how to get into and
        out of the model.

        {v
          Q1 -->-.         ,-->-- R1
                  \       /
          Q2 -->-- Q --> R --->-- R2
                  /       \
          Q3 -->-'         `-->-- R3
        v}
    *)
    module Make (Model : sig
                   val name : string  (* the name of the Rpc's being unified in the model *)
                   type query
                   type response
                 end) : sig

      (** Add a new version to the set of versions implemented by [implement_multi]. *)
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

  module Pipe_rpc : sig

    module type S = sig
      type query
      type response
      type error

      (** implement multiple versions at once *)
      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
            -> query
            -> aborted:unit Deferred.t
            -> (response Pipe.Reader.t, error) Result.t Deferred.t)
        -> 'state Implementation.t list

      (** All versions supported by [dispatch_multi].
          (useful for computing which old versions may be pruned) *)
      val versions : unit -> Int.Set.t
    end

    (** Given a model of the types involved in a family of Pipe_RPCs, this functor
        provides a single multi-version implementation function [implement_multi] in terms
        of that model and a mechanism for registering the individual versions that
        [implement_multi] knows about.  Registration requires knowing how to get into and
        out of the model.

       {v
          Q1 -->-.         ,-->-- R1
                  \       /
          Q2 -->-- Q --> R --->-- R2
                  /       \
          Q3 -->-'         `-->-- R3
        v}
    *)
    module Make (Model : sig
                   val name : string  (* the name of the Rpc's being unified in the model *)
                   type query
                   type response
                   type error
                 end) : sig

      (** add a new version to the set of versions available via [implement_multi]. *)
      module Register (Version_i : sig
                         val version : int
                         type query with bin_io
                         type response with bin_io
                         type error with bin_io
                         val model_of_query : query -> Model.query
                         val response_of_model : Model.response -> response
                         val error_of_model : Model.error -> error
                       end) : sig
        val rpc : (Version_i.query, Version_i.response, Version_i.error) Pipe_rpc.t
      end

      include S with type query    := Model.query
                 and type response := Model.response
                 and type error    := Model.error
    end
  end
end

module Both_convert : sig

  (** [Both_convert] rpcs combine features of both caller-converts and callee-converts
      versioning schemes in such a way that one can smoothly add a new version of the rpc
      to a shared library, and it doesn't matter whether the callee or caller upgrades to
      the latest version of the shared library first, the new version will not be
      exercised until both sides support it.

      {v
                     (conv)   (conv)                          (conv)   (conv)
                     caller   callee                          callee   caller
                     |        |                               |        |
                     |        |                               |        |
        Q.caller ---->-- Q1 -->-.             (impl)        .->-- R1 -->---- R.caller
                \                \            callee       /                /
                 \--->-- Q2 -->---\           |           /--->-- R2 -->---/
                  \                \          |          /                /
                   `->-- Q3 -->---- Q.callee --> R.callee ---->-- R3 -->-'
      v}
  *)

  module Plain : sig
    module type S = sig
      type caller_query
      type caller_response
      type callee_query
      type callee_response

      (** multi-version dispatch *)
      val dispatch_multi
        : Connection_with_menu.t -> caller_query -> caller_response Or_error.t Deferred.t

      (** implement multiple versions at once *)
      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> callee_query -> callee_response Deferred.t)
        -> 'state Implementation.t list

      (** All supported versions.  Useful for detecting old versions that may be pruned. *)
      val versions : unit -> Int.Set.t

    end


    module Make (Model : sig
                   val name : string
                   module Caller : sig type query type response end
                   module Callee : sig type query type response end
                 end) : sig

      open Model

      module Register
               (Version : sig
                  val version : int
                  type query    with bin_io
                  type response with bin_io
                  val query_of_caller_model : Caller.query -> query
                  val callee_model_of_query :                 query -> Callee.query
                  val response_of_callee_model : Callee.response -> response
                  val caller_model_of_response :                    response -> Caller.response
                end) : sig
      end

      include S with type caller_query    := Caller.query
                 and type caller_response := Caller.response
                 and type callee_query    := Callee.query
                 and type callee_response := Callee.response

    end
  end
end
