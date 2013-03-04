
(** [Async.Command] is [Core.Command] with additional async functions. *)

open Import

include module type of Core.Std.Command
  with type t      = Core.Std.Command.t
  with module Spec = Core.Std.Command.Spec

(** [async_basic] is exactly the same as [Core.Command.basic], except that the function it
    wraps returns [unit Deferred.t], instead of [unit].  [async_basic] will also start the
    Async scheduler before the wrapped function is run, and will stop the scheduler when
    the wrapped function returns. *)
val async_basic
  :  summary:string
  -> ?readme:(unit -> string)
  -> ('a, unit -> unit Deferred.t) Spec.t
  -> 'a
  -> t
