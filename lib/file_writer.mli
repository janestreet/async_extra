(* [File_writer] is a thin wrapper around [Writer] with a couple of extra features:

     * It keeps track of all the file writers that have been created so that it can
       iterate over them to find out how many bytes in total they have to write.
     * It keeps track of whether the underlying writer has failed, and if so silently
       ignores future operations.  This can prevent pointlessly filling up a writer's
       buffer with data that will never go anywhere. *)


open Core.Std
open Import

type t with sexp_of

(** [create file] opens [file], creating it if it doesn't exist. *)
val create : ?append:bool (* default true *) -> string -> t Deferred.t

(** [write t s] writes [s] to the file. *)
val write : t -> string -> unit

val write_substring : t -> Substring.t -> unit
val write_bigsubstring : t -> Bigsubstring.t -> unit
val schedule_bigstring : t -> Bigstring.t -> unit

val write_bin_prot : t -> 'a Bin_prot.Type_class.writer -> 'a -> unit

val write_sexp : ?hum:bool -> t -> Sexp.t -> unit

val monitor : t -> Monitor.t

(** [bytes_to_write ()] returns the sum over all async_file_writers of how
    many bytes they need to write. *)
val bytes_to_write : unit -> int

val flushed : t -> unit Deferred.t

val close : t -> unit Deferred.t
