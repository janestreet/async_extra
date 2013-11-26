(* A stream of messages processed in order and in lock step by a set of subscribers.

   A bus is useful to:

   - break circular dependencies in module communication (i.e. module A needs to update
     module B and module B needs to update module A).

   - collect related information (e.g. a stream of monitoring events) from many
   different parts of a program.

   A bus is analogous to a single topic in pub/sub systems and may have multiple readers
   and writers.

   It is guaranteed that all subscribers will process all updates in order and that no
   subscriber will process update (n + 1) before all subscribers have processed update n.

   A bus is either "unstarted" or "started".  An unstarted bus queues messages written to
   it.  No subscriber will see messages until [start] is called; any subscriber created
   before [start] is called will see all messages on the bus, even those written before
   its subscription.
*)

open Core.Std
open Import

type 'a t with sexp_of

include Invariant.S1 with type 'a t := 'a t

(** [create] creates a new, unstarted, bus.

    [can_subscribe_after_start] determines whether [subscribe_exn] succeeds after the bus
    is started. *)
val create : can_subscribe_after_start:bool -> 'a t

(** [start t] starts the bus; it starts delivery of queued messages to readers.  [start]
    will not run any subscribers; it just creates the async job that will start running
    them. *)
val start : _ t -> unit

(** [flushed t] returns a deferred that becomes determined when all subscribers have
    processed all values previously writen to [t]. *)
val flushed : _ t -> unit Deferred.t

(** [write t a] enqueues [a] on the bus, but does not call any subscriber functions
    immediately.  Multiple [write] calls in the same async cycle are efficient (i.e. they
    don't create a deferred per item). *)
val write : 'a t -> 'a -> unit

(** [subscribe_exn t ~f] causes [f] to be applied to all values subsequently written to
    [t] (or if [t] is unstarted, to prior values as well).  [subscribe_exn] raises if it
    is called on a started bus with [not can_subscribe_after_start].  The function [f] is
    allowed to call other [Bus] functions on [t], e.g. [write], [subscribe_exn], or
    [unsubscribe].

    If [f] raises, the corresponding subscriber will be automatically unsubscribed, and
    the exception will be sent to the monitor in effect when [subscribe_exn] was called.

    Once [unsubscribe t subscriber] is called, [f] will never be called again. *)
module Subscriber : sig type 'a t with sexp_of end

val subscribe_exn : 'a t -> f:('a -> unit) -> 'a Subscriber.t
val unsubscribe   : 'a t -> 'a Subscriber.t -> unit

(** [reader_exn t] returns a pipe that contains all elements subsequently written to [t]
    (and including prior values, if [t] is unstarted).  The difference with
    [subscribe_exn] is that the consumer may be an arbitrary amount behind other
    subscribers/consumers.  Pushback on the pipe is not honored.  Closing the reader is
    equivalent to calling [unsubscribe]. *)
val reader_exn : 'a t -> 'a Pipe.Reader.t
