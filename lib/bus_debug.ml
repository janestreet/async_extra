open Core.Std
open Async_core.Std

module Debug (Bus : module type of Bus) = struct

  module Debug = Debug.Make (struct end)

  include Debug

  open Bus

  type nonrec 'a t = 'a t with sexp_of

  let invariant = invariant

  let debug x = debug (invariant ignore) ~module_name:"Bus" x

  let create ~can_subscribe_after_start =
    debug "create" [ ] can_subscribe_after_start <:sexp_of< bool >> <:sexp_of< _ t >>
      (fun () -> create ~can_subscribe_after_start)
  ;;

  let start t =
    debug "start" [ t ] t <:sexp_of< _ t >> <:sexp_of< unit >>
      (fun () -> start t)
  ;;

  let flushed t =
    debug "flushed" [ t ] t <:sexp_of< _ t >> <:sexp_of< unit Deferred.t >>
      (fun () -> flushed t)
  ;;

  let write t a =
    debug "write" [ t ] t <:sexp_of< _ t >> <:sexp_of< unit >>
      (fun () -> write t a)
  ;;

  module Subscriber = struct
    open Subscriber
    type nonrec 'a t = 'a t with sexp_of
  end

  let subscribe_exn t ~f =
    debug "subscribe_exn" [ t ] t <:sexp_of< _ t >> <:sexp_of< _ Subscriber.t >>
      (fun () -> subscribe_exn t ~f)
  ;;

  let unsubscribe t subscriber =
    debug "unsubscribe" [ t ] (t, subscriber)
      <:sexp_of< _ t * _ Subscriber.t >> <:sexp_of< unit >>
      (fun () -> unsubscribe t subscriber)
  ;;

  let reader_exn t =
    debug "reader_exn" [ t ] t <:sexp_of< _ t >> <:sexp_of< _ Pipe.Reader.t >>
      (fun () -> reader_exn t)
  ;;
end
