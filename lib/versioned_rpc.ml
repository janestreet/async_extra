open Core.Std
open Import

open Rpc

exception Convert of
  [`Query | `Response | `Error | `State | `Update ]
  * [`Rpc of string]
  * [`Version of int] * exn
with sexp

exception Multiple_registrations of [`Rpc of string] * [`Version of int] with sexp

module Caller_converts = struct

  module Rpc = struct

    module type S = sig
      type query
      type response
      val dispatch_multi :
        version:int -> Connection.t -> query -> (response, exn) Result.t Deferred.t
      val versions : unit -> Int.Set.t
    end

    module Make (Model : sig
      val name : string
      type query
      type response
    end) = struct

      let name = Model.name

      let registry = Int.Table.create ~size:1 ()

      exception Unknown_version of string * int with sexp

      let dispatch_multi ~version conn query =
        match Hashtbl.find registry version with
        | None -> return (Error (Unknown_version (name, version)))
        | Some dispatch -> dispatch conn query

      let versions () = Int.Set.of_list (Int.Table.keys registry)

      module Register (Version_i : sig
        type query with bin_io
        type response with bin_io
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
              return (Error (Convert (`Query, `Rpc name, `Version version, exn)))
            | Ok q ->
              Rpc.dispatch rpc conn q
              >>| fun result ->
              Result.bind result (fun r ->
                match Result.try_with (fun () -> Version_i.model_of_response r) with
                | Ok r -> Ok r
                | Error exn ->
                  Error (Convert (`Response, `Rpc name, `Version version, exn)))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.replace registry ~key:version ~data:dispatch
          | Some _ -> raise (Multiple_registrations (`Rpc name, `Version version))

      end

    end

  end

  module Pipe_rpc = struct

    module type S = sig
      type query
      type response
      type error
      val dispatch_multi :
        version:int
        -> Connection.t
        -> query
        -> ( (response Or_error.t Pipe.Reader.t * Pipe_rpc.Id.t, error) Result.t
           , exn
           ) Result.t Deferred.t
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

      exception Unknown_version of string * int with sexp

      let dispatch_multi ~version conn query =
        match Hashtbl.find registry version with
        | None -> return (Error (Unknown_version (name, version)))
        | Some dispatch -> dispatch conn query

      let versions () = Int.Set.of_list (Int.Table.keys registry)

      module Register (Version_i : sig
        type query with bin_io
        type response with bin_io
        type error with bin_io
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
              return (Error (Convert (`Query, `Rpc name, `Version version, exn)))
            | Ok q ->
              Pipe_rpc.dispatch rpc conn q
              >>| fun result ->
                match result with
                | Error exn -> Error exn
                | Ok (Error e) ->
                  (match Result.try_with (fun () -> Version_i.model_of_error e) with
                  | Ok e' -> Ok (Error e')
                  | Error exn ->
                    Error (Convert (`Error, `Rpc name, `Version version, exn)))
                | Ok (Ok (pipe, id)) ->
                  let pipe =
                    Pipe.map pipe ~f:(fun r ->
                      match Result.try_with (fun () -> Version_i.model_of_response r) with
                      | Ok r -> Ok r
                      | Error exn ->
                        Error
                          (Error.of_exn
                            (Convert (`Response, `Rpc name, `Version version, exn))))
                  in
                  Ok (Ok (pipe, id))
          in
          match Hashtbl.find registry version with
          | None -> Hashtbl.replace registry ~key:version ~data:dispatch
          | Some _ -> raise (Multiple_registrations (`Rpc name, `Version version))

      end

    end

  end

end

module Callee_converts = struct

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
      type query with bin_io
      type response with bin_io
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
              raise (Convert (`Query, `Rpc name, `Version version, exn))
            | Ok q ->
              f s q
              >>| fun r ->
              match Result.try_with (fun () -> Version_i.response_of_model r) with
              | Ok r -> r
              | Error exn ->
                raise (Convert (`Response, `Rpc name, `Version version, exn))
          )
        in
        match Hashtbl.find registry version with
        | None -> Hashtbl.replace registry ~key:version ~data:{implement}
        | Some _ -> raise (Multiple_registrations (`Rpc name, `Version version))
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

  include Callee_converts.Make (Model)

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

  let supported_versions t ~rpc_name =
    Option.value ~default:Int.Set.empty (Hashtbl.find t rpc_name)

  let request conn =
    Rpc.dispatch Current_version.rpc conn ()
    >>| fun result ->
    Result.map result ~f:(fun entries ->
      Hashtbl.map ~f:Int.Set.of_list (String.Table.of_alist_multi entries))

end

