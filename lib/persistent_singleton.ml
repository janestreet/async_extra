(** Implements a value that is either in a file, or in memory, but not both. Is used by
    live and the friend to store sequence numbers and counters. If the value is moved to
    memory, changed, and then the process crashes, the file will correctly reflect that
    the value has been lost. *)

open Core.Std
open Import

module type Arg = Sexpable

module type S = sig
  type persistent_singleton

  val load :
    string
    -> default:persistent_singleton
    -> persistent_singleton Deferred.t

  val save :
    string
    -> value:persistent_singleton
    -> unit Deferred.t
end

module Make (Z : Arg) : S with type persistent_singleton = Z.t = struct
  type persistent_singleton = Z.t

  let save_sexp file option =
    Writer.save_sexp file (Option.sexp_of_t Z.sexp_of_t option)
  ;;

  exception Can_not_determine_whether_file_exists of string with sexp
  exception Can_not_load_due_to_unclean_shutdown of string with sexp

  let load file ~default =
    let res =
      Sys.file_exists file
      >>= function
        | `Yes -> Reader.load_sexp_exn file (Option.t_of_sexp Z.t_of_sexp)
        | `No -> return (Some default)
        | `Unknown -> raise (Can_not_determine_whether_file_exists file)
    in
    res
    >>= fun res ->
    save_sexp file None
    >>| fun () ->
    match res with
    | None -> raise (Can_not_load_due_to_unclean_shutdown file)
    | Some res -> res
  ;;

  let save file ~value = save_sexp file (Some value)
end
