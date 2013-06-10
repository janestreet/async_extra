open Core.Std
open Import

open Rpc

let failed_conversion x =
  Error.create "type conversion failure" x
    <:sexp_of<
      [`Query | `Response | `Error | `State | `Update ]
      * [`Rpc of string]
      * [`Version of int]
      * exn
    >>

let multiple_registrations x =
  Error.create "multiple rpc registrations" x
    <:sexp_of< [`Rpc of string] * [`Version of int] >>

let unknown_version x =
  Error.create "unknown rpc version" x
    <:sexp_of< string * int >>

module Callee_converts = struct

  module Rpc = struct

    module type S = sig
      type query
      type response
      val implement_multi :
        ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> query -> response Deferred.t)
        -> 'state Implementation.t list
      val versions : unit -> Int.Set.t
    end

    module Make (Model : sig
      val name : string
      type query
      type response
    end) = struct

      let name = Model.name

      type 's impl = 's -> Model.query -> Model.response Deferred.t

      type implementer =
        { implement : 's. log_version:(int -> unit) -> 's impl -> 's Implementation.t }

      let registry : (int, implementer) Hashtbl.t = Int.Table.create ~size:1 ()

      let implement_multi ?log_not_previously_seen_version f =
        let log_version =
          match log_not_previously_seen_version with
          | None -> ignore
          (* prevent calling [f] more than once per version *)
          | Some f -> Memo.general (f ~name)
        in
        List.map (Hashtbl.data registry) ~f:(fun i -> i.implement ~log_version f)

      let versions () = Int.Set.of_list (Hashtbl.keys registry)

      module Register (Version_i : sig
        type query with bin_type_class
        type response with bin_type_class
        val version : int
        val model_of_query : query -> Model.query
        val response_of_model : Model.response -> response
      end) = struct

        open Version_i

        let rpc = Rpc.create ~name ~version ~bin_query ~bin_response

        let () =
          let implement ~log_version f =
            Rpc.implement rpc (fun s q ->
              log_version version;
              match Result.try_with (fun () -> Version_i.model_of_query q) with
              | Error exn ->
                Error.raise
                  (failed_conversion (`Query, `Rpc name, `Version version, exn))
              | Ok q ->
                f s q
                >>| fun r ->
                match Result.try_with (fun () -> Version_i.response_of_model r) with
                | Ok r -> r
                | Error exn ->
                  Error.raise
                    (failed_conversion (`Response, `Rpc name, `Version version, exn))
            )
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.replace registry ~key:version ~data:{implement}
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))
      end

    end

  end

  module Pipe_rpc = struct

    module type S = sig
      type query
      type response
      type error
      val implement_multi :
        ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state
          -> query
          -> aborted:unit Deferred.t
          -> (response Pipe.Reader.t, error) Result.t Deferred.t)
        -> 'state Implementation.t list
      val versions : unit -> Int.Set.t
    end

    module Make (Model : sig
      val name : string
      type query
      type response
      type error
    end) = struct

      let name = Model.name

      type 's impl =
        's
        -> Model.query
        -> aborted:unit Deferred.t
        -> (Model.response Pipe.Reader.t, Model.error) Result.t Deferred.t

      type implementer =
        { implement : 's. log_version:(int -> unit) -> 's impl -> 's Implementation.t }

      let registry = Int.Table.create ~size:1 ()

      let implement_multi ?log_not_previously_seen_version f =
        let log_version =
          match log_not_previously_seen_version with
          | None -> ignore
          (* prevent calling [f] more than once per version *)
          | Some f -> Memo.general (f ~name)
        in
        List.map (Hashtbl.data registry) ~f:(fun i -> i.implement ~log_version f)

      let versions () = Int.Set.of_list (Int.Table.keys registry)

      module Register (Version_i : sig
        type query with bin_type_class
        type response with bin_type_class
        type error with bin_type_class
        val version : int
        val model_of_query : query -> Model.query
        val response_of_model : Model.response -> response
        val error_of_model : Model.error -> error
      end) = struct

        open Version_i

        let rpc = Pipe_rpc.create ~name ~version ~bin_query ~bin_response ~bin_error

        let () =
          let implement ~log_version f =
            Pipe_rpc.implement rpc (fun s q ~aborted ->
              log_version version;
              match Result.try_with (fun () -> Version_i.model_of_query q) with
              | Error exn ->
                Error.raise
                  (failed_conversion (`Response, `Rpc name, `Version version, exn))
              | Ok q ->
                f s q ~aborted
                >>| function
                | Ok pipe ->
                  Ok (Pipe.map pipe ~f:(fun r ->
                    match Result.try_with (fun () -> Version_i.response_of_model r) with
                    | Ok r -> r
                    | Error exn ->
                      Error.raise
                        (failed_conversion (`Response, `Rpc name, `Version version, exn))))
                | Error error ->
                  match Result.try_with (fun () -> Version_i.error_of_model error) with
                  | Ok error -> Error error
                  | Error exn ->
                    Error.raise
                      (failed_conversion (`Error, `Rpc name, `Version version, exn))
            )
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.replace registry ~key:version ~data:{implement}
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))

      end

    end

  end

end

module Menu = struct

  (***************** some prohibitions for this module ******************

    (1) !!! never prune old versions of this rpc !!!

        It is too fundamental to the workings of various versioning
        schemes and it probably won't change very much anyway.

    (2) !!! only ever say "with bin_io" on built-in ocaml types !!!

        Examples of built-in types are int, list, string, etc.

        This is to protect ourselves against changes to Core data
        structures, for example.

   *********************************************************************)

  type rpc_name = string
  type rpc_version = int

  module Model = struct
    let name = "__Versioned_rpc.Menu"
    type query = unit
    type response = (rpc_name * rpc_version) list
  end

  include Callee_converts.Rpc.Make (Model)

  module V1 = struct
    module T = struct
      let version = 1
      type query = unit with bin_io
      type response = (string * int) list with bin_io
      let model_of_query q = q
      let response_of_model r = r
    end
    include T
    include Register (T)
  end

  module Current_version = V1

  let add impls =
    let menu =
      List.map impls ~f:(fun i ->
        match Implementation.description i with
          {Implementation.Description.name; version} -> (name, version))
    in
    let menu_impls = implement_multi (fun _ () -> return menu) in
    impls @ menu_impls

  type t = Int.Set.t String.Table.t

  let supported_rpcs t =
    let open List.Monad_infix in
    String.Table.to_alist t
    >>= fun (name, versions) ->
    Int.Set.to_list versions
    >>| fun version ->
    {Implementation.Description.name; version}

  let supported_versions t ~rpc_name =
    Option.value ~default:Int.Set.empty (Hashtbl.find t rpc_name)

  let request conn =
    Rpc.dispatch Current_version.rpc conn ()
    >>| fun result ->
    Result.map result ~f:(fun entries ->
      Hashtbl.map ~f:Int.Set.of_list (String.Table.of_alist_multi entries))

end

module Connection_with_menu = struct

  type t = {
    connection : Connection.t;
    menu : Menu.t;
  } with fields

  let create connection =
    let open Deferred.Or_error.Monad_infix in
    Menu.request connection
    >>| fun menu ->
    {connection; menu}

end

module Caller_converts = struct

  let most_recent_common_version ~rpc_name ~caller_versions ~callee_versions =
    match Set.max_elt (Set.inter callee_versions caller_versions) with
    | Some version -> Ok version
    | None ->
      Or_error.error "caller and callee share no common versions for rpc"
        ( `Rpc rpc_name
        , `Caller_versions caller_versions
        , `Callee_versions callee_versions )
        (<:sexp_of< [`Rpc of string]
                  * [`Caller_versions of Int.Set.t]
                  * [`Callee_versions of Int.Set.t] >>)

  let dispatch_with_specific_version ~version ~connection ~name ~query ~registry =
    match Hashtbl.find registry version with
    | None -> return (Error (unknown_version (name, version)))
    | Some dispatch -> dispatch connection query

  let dispatch_with_version_menu {Connection_with_menu.connection; menu} query
        ~name ~versions ~registry =
    let open Deferred.Or_error.Monad_infix in
    let callee_versions = Menu.supported_versions menu ~rpc_name:name in
    let caller_versions = versions () in
    return (most_recent_common_version ~rpc_name:name ~caller_versions ~callee_versions)
    >>= fun version ->
    dispatch_with_specific_version ~version ~connection ~name ~query ~registry

  module Rpc = struct

    module type S = sig
      type query
      type response
      val deprecated_dispatch_multi :
          version:int -> Connection.t -> query -> response Or_error.t Deferred.t

      val dispatch_multi :
        Connection_with_menu.t -> query -> response Or_error.t Deferred.t
      val versions : unit -> Int.Set.t
    end

    module Make (Model : sig
                   val name : string
                   type query
                   type response
                 end) = struct

      let name = Model.name

      let registry = Int.Table.create ~size:1 ()

      let versions () = Int.Set.of_list (Int.Table.keys registry)

      let dispatch_multi conn_with_menu query =
        dispatch_with_version_menu conn_with_menu query ~name ~versions ~registry

      let deprecated_dispatch_multi ~version connection query =
        dispatch_with_specific_version ~version ~connection ~query ~name ~registry

      module Register (Version_i : sig
                         type query with bin_type_class
                         type response with bin_type_class
                         val version : int
                         val query_of_model : Model.query -> query
                         val model_of_response : response -> Model.response
                       end) = struct

        open Version_i

        let rpc = Rpc.create ~name ~version ~bin_query ~bin_response

        let () =
          let dispatch conn q =
            match Result.try_with (fun () -> Version_i.query_of_model q) with
            | Error exn ->
              return
                (Error (failed_conversion (`Query, `Rpc name, `Version version, exn)))
            | Ok q ->
              Rpc.dispatch rpc conn q
              >>| fun result ->
              Result.bind result (fun r ->
                match Result.try_with (fun () -> Version_i.model_of_response r) with
                | Ok r -> Ok r
                | Error exn ->
                  Error (failed_conversion (`Response, `Rpc name, `Version version, exn)))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.replace registry ~key:version ~data:dispatch
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))

      end

    end

  end

  module Pipe_rpc = struct

    module type S = sig
      type query
      type response
      type error
      val deprecated_dispatch_multi :
        version:int -> Connection.t -> query ->
          ( response Or_error.t Pipe.Reader.t * Pipe_rpc.Id.t
           , error
           ) Result.t Or_error.t Deferred.t

      val dispatch_multi :
        Connection_with_menu.t
        -> query
        -> ( response Or_error.t Pipe.Reader.t * Pipe_rpc.Id.t
           , error
           ) Result.t Or_error.t Deferred.t
      val versions : unit -> Int.Set.t
    end

    module Make (Model : sig
                   val name : string
                   type query
                   type response
                   type error
                 end) = struct

      let name = Model.name

      let registry = Int.Table.create ~size:1 ()

      let versions () = Int.Set.of_list (Int.Table.keys registry)

      let dispatch_multi conn_with_menu query =
        dispatch_with_version_menu conn_with_menu query ~name ~versions ~registry

      let deprecated_dispatch_multi ~version connection query =
        dispatch_with_specific_version ~version ~connection ~query ~name ~registry

      module Register (Version_i : sig
        type query with bin_type_class
        type response with bin_type_class
        type error with bin_type_class
        val version : int
        val query_of_model : Model.query -> query
        val model_of_response : response -> Model.response
        val model_of_error : error -> Model.error
      end) = struct

        open Version_i

        let rpc = Pipe_rpc.create ~name ~version ~bin_query ~bin_response ~bin_error

        let () =
          let dispatch conn q =
            match Result.try_with (fun () -> Version_i.query_of_model q) with
            | Error exn ->
              return
                (Error (failed_conversion (`Query, `Rpc name, `Version version, exn)))
            | Ok q ->
              Pipe_rpc.dispatch rpc conn q
              >>| fun result ->
                match result with
                | Error exn -> Error exn
                | Ok (Error e) ->
                  (match Result.try_with (fun () -> Version_i.model_of_error e) with
                  | Ok e' -> Ok (Error e')
                  | Error exn ->
                    Error (failed_conversion (`Error, `Rpc name, `Version version, exn)))
                | Ok (Ok (pipe, id)) ->
                  let pipe =
                    Pipe.map pipe ~f:(fun r ->
                      match Result.try_with (fun () -> Version_i.model_of_response r) with
                      | Ok r -> Ok r
                      | Error exn ->
                        Error
                          (failed_conversion
                             (`Response, `Rpc name, `Version version, exn)))
                  in
                  Ok (Ok (pipe, id))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.replace registry ~key:version ~data:dispatch
          | Some _ -> Error.raise (multiple_registrations (`Rpc name, `Version version))

      end

    end

  end

end

module Both_convert = struct

  module Plain = struct
    module type S = sig
      type caller_query
      type callee_query
      type caller_response
      type callee_response

      val dispatch_multi :
        Connection_with_menu.t -> caller_query -> caller_response Or_error.t Deferred.t

      val implement_multi
        :  ?log_not_previously_seen_version:(name:string -> int -> unit)
        -> ('state -> callee_query -> callee_response Deferred.t)
        -> 'state Implementation.t list

      val versions : unit -> Int.Set.t
    end

    module Make (Model : sig
      val name : string
      module Caller : sig type query type response end
      module Callee : sig type query type response end
    end) = struct
      open Model

      module Caller = Caller_converts.Rpc.Make (struct let name = name include Caller end)
      module Callee = Callee_converts.Rpc.Make (struct let name = name include Callee end)

      TEST = Int.Set.equal (Caller.versions ()) (Callee.versions ())

      module Register (Version : sig
        open Model
        val version : int
        type query    with bin_io
        type response with bin_io
        val query_of_caller_model : Caller.query -> query
        val callee_model_of_query :                 query -> Callee.query
        val response_of_callee_model : Callee.response -> response
        val caller_model_of_response :                    response -> Caller.response
      end) = struct
        include Callee.Register (struct
          include Version
          let model_of_query    = callee_model_of_query
          let response_of_model = response_of_callee_model
        end)
        include Caller.Register (struct
          include Version
          let query_of_model    = query_of_caller_model
          let model_of_response = caller_model_of_response
        end)
        TEST = Int.Set.equal (Caller.versions ()) (Callee.versions ())
      end

      let dispatch_multi = Caller.dispatch_multi

      let implement_multi = Callee.implement_multi

      (* Note: Caller.versions is the same as Callee.versions, so it doesn't matter which
        one we call here *)
      let versions () = Caller.versions ()
    end
  end
end
