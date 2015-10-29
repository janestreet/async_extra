(** [Async.Command] is [Core.Command] with additional Async functions. *)

open Import

include module type of Core.Std.Command
  with type t      = Core.Std.Command.t
  with module Spec = Core.Std.Command.Spec

(** [async] is like [Core.Command.basic], except that the main function it expects returns
    [unit Deferred.t], instead of [unit].  [async] will also start the Async scheduler
    before main is run, and will stop the scheduler when main returns. *)
val async  : ('a, unit Deferred.t) basic_command
val async' : ('a, unit Deferred.t) basic_command'

(** [async_basic] is a deprecated synonym for [async] that will eventually go away.  It is
    here to give code outside jane street a chance to switch over before we delete it. *)
val async_basic : ('a, unit Deferred.t) basic_command

(** [async_or_error] is like [async], except that the main function it expects may
    return an error, in which case it prints out the error message and shuts down with
    exit code 1. *)
val async_or_error : ('a, unit Deferred.Or_error.t) basic_command
val async_or_error' : ('a, unit Deferred.Or_error.t) basic_command'

(** To create an [Arg_type.t] that uses auto-completion and uses Async to compute the
    possible completions, one should use

    {[
      Arg_type.create ~complete of_string
    ]}

    where [complete] wraps its Async operations in [Thread_safe.block_on_async].  With
    this, the [complete] function is only called when the executable is auto-completing,
    not for ordinary execution.  This improves performance, and also means that the Async
    scheduler isn't started for ordinary execution of the command, which makes it possible
    for the command to daemonize (which requires the scheduler to not have been started).
*)
