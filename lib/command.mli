
(** [Async.Command] is [Core.Command] with additional async functions. *)

open Core.Std
open Import

include (module type of Core.Std.Command)

(** [async_basic] is exactly the same as [Core.Command.basic], except that the function it
    wraps returns [unit Deferred.t], instead of [unit].  [async_basic] will also start the
    Async scheduler before the wrapped function is run, and will stop the scheduler when
    the wrapped function returns. *)
val async_basic
  :  summary:string
  -> ?readme:(unit -> string)
  -> ('a, unit -> unit Deferred.t) Core.Std.Command.Spec.t
  -> 'a
  -> Core.Std.Command.t
