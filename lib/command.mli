
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

(** To create an [Arg_type.t] that uses auto-completion and uses async to compute the
    possible completions, one should use:

    {[
      Arg_type.create ~complete of_string
    ]}

    With this, the [complete] function is only called when the executable is
    auto-completing, not for ordinary execution.  This improves performance, and also
    means that the async scheduler isn't started for ordinary execution of the command,
    which makes it possible for the command to daemonize (which requires the scheduler to
    not have been started).
*)
