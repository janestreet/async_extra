open Core.Std
open Import

(** Wrapper around Core.Std.User_and_group with a deferred [for_this_process] /
    [for_this_process_exn]. *)

type t = Core.Std.User_and_group.t with sexp, bin_io
include Identifiable with type t := t
val create : user:string -> group:string -> t
val user : t -> string
val group : t -> string

val for_this_process : unit -> t Or_error.t Deferred.t
val for_this_process_exn : unit -> t Deferred.t
