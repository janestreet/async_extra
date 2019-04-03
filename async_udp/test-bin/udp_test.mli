open! Core
open! Async

val tests : (string * (unit -> unit Deferred.t)) list
