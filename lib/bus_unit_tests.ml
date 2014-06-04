open Core.Std
open Import

TEST_MODULE = (struct

  module Bus = Bus_debug.Debug (Bus)
  open Bus

  let _ = show_messages := false

  let async_unit_test = Thread_safe.block_on_async_exn

  module Subscriber = struct
    open Subscriber
    type nonrec 'a t = 'a t with sexp_of
  end

  type nonrec 'a t = 'a t with sexp_of

  let invariant = invariant

  let create  = create
  let flushed = flushed
  let start   = start
  let write   = write

  TEST_UNIT = (* [~can_subscribe_after_start:false] *)
    let t = create ~can_subscribe_after_start:false in
    start t;
    assert (does_raise (fun () -> subscribe_exn t ~f:ignore))
  ;;

  TEST_UNIT = (* [start] doesn't run subscribers. *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      let did_run = ref false in
      ignore (subscribe_exn t ~f:(fun () -> did_run := true) : _ Subscriber.t);
      start t;
      assert (not !did_run);
      write t ();
      flushed t
      >>= fun () ->
      assert !did_run;
      return ())
  ;;

  (* Messages are delivered in order.  Subscribers after [start] don't see messages
     enqueued before [start] is called.  Subscribers subscribing before [start] do. *)
  TEST_UNIT =
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:true in
      let q_before = Queue.create () in
      let q_after  = Queue.create () in
      let _subscriber = subscribe_exn t ~f:(fun v -> Queue.enqueue q_before v) in
      let l1 = List.init 10 ~f:ident in
      let l2 = List.map l1 ~f:(Int.( * ) 42) in
      start t;
      List.iter l1 ~f:(fun i -> write t i);
      flushed t
      >>= fun () ->
      let _ = subscribe_exn t ~f:(fun v -> Queue.enqueue q_after v) in
      List.iter l2 ~f:(fun i -> write t i);
      flushed t
      >>| fun () ->
      let l_before = Queue.to_list q_before in
      let l_after  = Queue.to_list q_after in
      assert (l1 @ l2 = l_before);
      assert (l2      = l_after));
  ;;

  TEST_UNIT = (* Calling [write] from a subscriber works *)
    async_unit_test (fun () ->
      let q = Queue.create () in
      let t = create ~can_subscribe_after_start:false in
      let (_ : _ Subscriber.t) =
        subscribe_exn t ~f:(fun v ->
          if v % 2 = 0
          then write t (v + 1)
          else Queue.enqueue q v)
      in
      start t;
      let l = List.init 10 ~f:ident in
      let expected = List.map l ~f:(fun v -> if v % 2 = 0 then v + 1 else v) in
      List.iter l ~f:(write t);
      flushed t
      >>| fun () ->
      assert (List.sort ~cmp:Int.compare expected
              = List.sort ~cmp:Int.compare (Queue.to_list q)));
  ;;

  let subscribe_exn = subscribe_exn
  let unsubscribe = unsubscribe

  TEST_UNIT = (* subscription before start sees message from before subscription *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      write t ();
      let subscriber_ran = ref false in
      let _subscriber = subscribe_exn t ~f:(fun () -> subscriber_ran := true) in
      assert (not !subscriber_ran);
      start t;
      assert (not !subscriber_ran);
      flushed t
      >>= fun () ->
      assert !subscriber_ran;
      return ())
  ;;

  TEST_UNIT = (* subscription after start fails with [~can_subscribe_after_start:false] *)
    let t = create ~can_subscribe_after_start:false in
    start t;
    assert (does_raise (fun () -> subscribe_exn t ~f:(fun _ -> assert false)));
  ;;

  TEST_UNIT = (* subscription after start sees messages after subscription *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:true in
      start t;
      write t ();
      let r = ref 0 in
      ignore (subscribe_exn t ~f:(fun () -> incr r) : _ Subscriber.t);
      write t ();
      flushed t
      >>= fun () ->
      assert (!r = 1);
      return ())
  ;;

  TEST_UNIT = (* unsubscription before start sees no messages *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      write t ();
      let subscriber_ran = ref false in
      let subscriber = subscribe_exn t ~f:(fun () -> subscriber_ran := true) in
      unsubscribe t subscriber;
      start t;
      flushed t
      >>= fun () ->
      assert (not !subscriber_ran);
      return ())
  ;;

  TEST_UNIT = (* a subscriber that raises is unsubscribed, errors go to the caller of
                 [subscribe_exn] *)
    async_unit_test (fun () ->
      let num_calls = ref 0 in
      let num_errors = ref 0 in
      let t = create ~can_subscribe_after_start:false in
      Monitor.handle_errors
        (fun () ->
           ignore (subscribe_exn t ~f:(fun () -> incr num_calls; failwith "")
                   : _ Subscriber.t);
           Deferred.unit)
        (fun _ -> incr num_errors);
      >>= fun () ->
      start t;
      assert (!num_calls  = 0);
      assert (!num_errors = 0);
      write t ();
      flushed t
      >>= fun () ->
      assert (!num_calls  = 1);
      assert (!num_errors = 1);
      write t ();
      flushed t
      >>= fun () ->
      assert (!num_calls  = 1);
      assert (!num_errors = 1);
      return ())
  ;;

  TEST_UNIT = (* asynchronous errors raised by [f] go to the right place *)
    async_unit_test (fun () ->
      let raise_after = Ivar.create () in
      let got_error = Ivar.create () in
      let t = create ~can_subscribe_after_start:false in
      Monitor.handle_errors
        (fun () ->
           ignore (subscribe_exn t ~f:(fun () ->
             upon (Ivar.read raise_after) (fun () -> failwith ""))
            : _ Subscriber.t);
           Deferred.unit)
        (fun _ -> Ivar.fill got_error ());
      >>= fun () ->
      assert (Ivar.is_empty got_error);
      start t;
      write t ();
      flushed t
      >>= fun () ->
      assert (Ivar.is_empty got_error);
      Ivar.fill raise_after ();
      Ivar.read got_error)
  ;;

  TEST_UNIT = (* immediate unsubscription *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      let subscriber = subscribe_exn t ~f:(fun () -> assert false) in
      unsubscribe t subscriber;
      start t;
      write t ();
      flushed t)
  ;;

  TEST_UNIT = (* unsubscription after [start] *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      let subscriber = subscribe_exn t ~f:(fun () -> assert false) in
      write t ();
      start t;
      unsubscribe t subscriber;
      flushed t)
  ;;

  TEST_UNIT = (* unsubscription from a subscriber *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      let num_calls = ref 0 in
      let r = ref None in
      r :=
        Some (subscribe_exn t ~f:(fun () ->
          incr num_calls;
          match !r with
          | None -> assert false
          | Some s -> r := None; unsubscribe t s));
      write t ();
      write t ();
      start t;
      flushed t
      >>= fun () ->
      assert (!num_calls = 1);
      return ())
  ;;

  TEST_UNIT = (* a subscriber that unsubscribes and then raises *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      let r = ref None in
      let monitor = Monitor.create () in
      ignore (Monitor.detach_and_get_error_stream monitor);
      r :=
        Scheduler.within_v ~monitor (fun () ->
          subscribe_exn t ~f:(fun () ->
            match !r with
            | None -> assert false
            | Some s -> r := None; unsubscribe t s; failwith ""));
      write t ();
      start t;
      flushed t)
  ;;

  TEST_UNIT = (* unsubscribing from the wrong bus raises *)
    let t1 = create ~can_subscribe_after_start:false in
    let t2 = create ~can_subscribe_after_start:false in
    let subscriber = subscribe_exn t1 ~f:(fun _ -> ()) in
    assert (does_raise (fun () -> unsubscribe t2 subscriber));
  ;;

  let reader_exn = reader_exn

  TEST_UNIT = (* readers don't block lockstep subscribers *)
    async_unit_test (fun () ->
      let t = create ~can_subscribe_after_start:false in
      write t 1;
      write t 2;
      let _r = reader_exn t in
      let subscriber_saw = ref 0 in
      let _subscriber = subscribe_exn t ~f:(fun x -> subscriber_saw := x) in
      start t;
      flushed t
      >>= fun () ->
      assert (!subscriber_saw = 2);
      return ())
  ;;
end
(* This signature constraint is here to remind us to add a unit test whenever the
   interface to [Bus] changes. *)
: module type of Bus)
