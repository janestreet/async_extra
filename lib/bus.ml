open Core.Std
open Import

let verbose = false

module State = struct
  type t =
    | Created
    | Started
    | Syncing
    with sexp_of
end

open State

module Subscriber = struct
  type 'a t =
    { mutable am_subscribed   : bool
    ; mutable subscribers_elt : 'a t sexp_opaque Bag.Elt.t option
    ; f                       : 'a -> unit
    ; send_f_raises_to        : Monitor.t
    }
  with fields, sexp_of

  let invariant _invariant_a t =
    Invariant.invariant _here_ t <:sexp_of< _ t >> (fun () ->
      let check f = Invariant.check_field t f in
      Fields.iter
        ~am_subscribed:ignore
        ~f:ignore
        ~send_f_raises_to:ignore
        ~subscribers_elt:(check (function
          | None -> ()
          | Some subscribers_elt ->
            assert (phys_equal t (Bag.Elt.value subscribers_elt)))))
  ;;
end

module Todo = struct
  type 'a t =
    | Flushed     of unit Ivar.t
    | Subscribe   of 'a Subscriber.t
    | Unsubscribe of 'a Subscriber.t
    | Write       of 'a
  with sexp_of

  let invariant invariant_a t =
    Invariant.invariant _here_ t <:sexp_of< _ t >> (fun () ->
      match t with
      | Write a                   -> invariant_a a;
      | Flushed fill_when_flushed -> assert (Ivar.is_empty fill_when_flushed);
      | Subscribe subscriber      -> Subscriber.invariant invariant_a subscriber
      | Unsubscribe subscriber    ->
        Subscriber.invariant invariant_a subscriber;
        assert (not subscriber.Subscriber.am_subscribed));
  ;;
end

type 'a t =
  { mutable state             : State.t
  ; can_subscribe_after_start : bool
  ; subscribers               : 'a Subscriber.t Bag.t
  ; todo                      : 'a Todo.t Queue.t
  }
with fields, sexp_of

let invariant invariant_a t =
  Invariant.invariant _here_ t <:sexp_of< _ t >> (fun () ->
    let check f = Invariant.check_field t f in
    Fields.iter
      ~can_subscribe_after_start:ignore
      ~state:(check (function Created | Started | Syncing -> ()))
      ~subscribers:(check (fun subscribers ->
        Bag.invariant subscribers;
        Bag.iter subscribers ~f:(Subscriber.invariant invariant_a)))
      ~todo:(check (fun todo ->
        Queue.iter todo ~f:(Todo.invariant invariant_a);
        match t.state with
        | Syncing -> ()
        | Started -> assert (Queue.is_empty t.todo)
        | Created ->
          Queue.iter todo ~f:(function
            | Todo.Flushed _ | Todo.Write _ -> ()
            | Todo.Subscribe _ | Todo.Unsubscribe _ -> assert false))))
;;

let create ~can_subscribe_after_start =
  { can_subscribe_after_start
  ; state       = Created
  ; subscribers = Bag.create ()
  ; todo        = Queue.create ()
  }
;;

let do_subscribe t subscriber =
  assert subscriber.Subscriber.am_subscribed;
  subscriber.Subscriber.subscribers_elt <- Some (Bag.add t.subscribers subscriber);
;;

let do_unsubscribe t subscriber =
  assert (not subscriber.Subscriber.am_subscribed);
  let module S = Subscriber in
  match subscriber.S.subscribers_elt with
  | None ->
    (* This can happen if the [unsubscribe t subscriber] is called before [Todo.Subscribe]
        is processed. *)
    ()
  | Some elt ->
    Bag.remove t.subscribers elt;
    subscriber.S.subscribers_elt <- None;
;;

let sync t =
  if verbose then Core.Std.Debug.eprints "sync" t <:sexp_of< _ t >>;
  match t.state with
  | Created | Syncing -> ()
  | Started           ->
    t.state <- Syncing;
    upon Deferred.unit (fun () ->
      while not (Queue.is_empty t.todo) do
        match Queue.dequeue_exn t.todo with
        | Todo.Flushed fill_when_flushed -> Ivar.fill fill_when_flushed ();
        | Todo.Subscribe subscriber ->
          if subscriber.Subscriber.am_subscribed then do_subscribe t subscriber;
        | Todo.Unsubscribe subscriber -> do_unsubscribe t subscriber;
        | Todo.Write a ->
          Bag.iter t.subscribers ~f:(fun subscriber ->
            let { Subscriber. am_subscribed; send_f_raises_to; f; _ } = subscriber in
            if am_subscribed then
              match Scheduler.within_v ~monitor:send_f_raises_to (fun () -> f a) with
              | Some () -> ()
              | None    ->
                if subscriber.Subscriber.am_subscribed then begin
                  subscriber.Subscriber.am_subscribed <- false;
                  Queue.enqueue t.todo (Todo.Unsubscribe subscriber);
                end);
      done;
      t.state <- Started)
;;

let enqueue t todo = Queue.enqueue t.todo todo; sync t

let flushed t = Deferred.create (fun ivar -> enqueue t (Todo.Flushed ivar))

let write t a = enqueue t (Todo.Write a)

let start t =
  match t.state with
  | Started | Syncing -> ()
  | Created           ->
    t.state <- Started;
    if not (Queue.is_empty t.todo) then sync t;
;;

let fail_if_not_can_subscribe_after_start t =
  if not t.can_subscribe_after_start
  then failwiths
         "cannot subscribe to started bus created with ~can_subscribe_after_start:false"
         t <:sexp_of< _ t >>
;;

let subscribe_exn t ~f =
  let subscriber =
    { Subscriber.
      am_subscribed    = true
    ; f
    ; send_f_raises_to = Monitor.current ()
    ; subscribers_elt  = None
    }
  in
  begin match t.state with
  (* If [t.state = Created], we [do_subscribe] now so that subscriptions before [start]
     see all messages.  If [t.state = Started], we [do_subscribe] now since [t.todo] is
     empty and we are not running a subscriber.  If [t.state = Started] or [t.state =
     Syncing] we need to check [t.can_subscribe_after_start] *)
  | Created -> do_subscribe t subscriber;
  | Started ->
    fail_if_not_can_subscribe_after_start t; do_subscribe t subscriber
  | Syncing ->
    fail_if_not_can_subscribe_after_start t; enqueue t (Todo.Subscribe subscriber)
  end;
  subscriber;
;;

let unsubscribe t subscriber =
  if subscriber.Subscriber.am_subscribed then begin
    subscriber.Subscriber.am_subscribed <- false;
    match t.state with
    (* If [t.state = Created] or [t.state = Started], we can [do_unsubscribe] now
       since we can not be running a subscriber. *)
    | Created | Started -> do_unsubscribe t subscriber
    | Syncing -> enqueue t (Todo.Unsubscribe subscriber)
  end;
;;

let reader_exn t =
  let r, w = Pipe.create () in
  let token =
    subscribe_exn t ~f:(fun x ->
      if not (Pipe.is_closed w) then Pipe.write_without_pushback w x)
  in
  upon (Pipe.closed w) (fun () -> unsubscribe t token);
  r
;;
