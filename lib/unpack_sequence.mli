(** [Unpack_sequence] implements a way to take an [unpack_one] function that can unpack a
    value from a character buffer, and use it to unpack a sequence of packed values
    coming via a string pipe into a pipe of upacked values. *)
open Core.Std
open Import

module Result : sig
  type ('a, 'b) t =
  | Input_closed
  | Input_closed_in_the_middle_of_data of ('a, 'b) Unpack_buffer.t
  | Output_closed of 'a Queue.t * ('a, 'b) Unpack_buffer.t
  | Unpack_error of Error.t
  with sexp_of

  val to_error : (_, _) t -> Error.t
end

val unpack_from_string_pipe
  :  ('a, 'b) Unpack_buffer.t
  -> string Pipe.Reader.t
  -> 'a Pipe.Reader.t * ('a, 'b) Result.t Deferred.t

(** [unpack_from_reader unpacker] unpacks all the values from reader until reaching EOF.
    The resulting [pipe] is closed once unpacking finishes (normally or abnormally). *)
val unpack_from_reader
  :  ('a, 'b) Unpack_buffer.t
  -> Reader.t
  -> 'a Pipe.Reader.t * ('a, 'b) Result.t Deferred.t

val unpack_bin_prot_from_reader
  : 'a Bin_prot.Type_class.reader
  -> Reader.t
  -> 'a Pipe.Reader.t * ('a, unit) Result.t Deferred.t
