open! Core
open! Async
open! Import
open! Expect_test_helpers_base
open! Bus
open! Async_bus

let () = Backtrace.elide := true

let%expect_test "[first_exn]" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Allow
      ~on_callback_raise:Error.raise
  in
  let d = first_exn bus [%here] Arity1 ~f:(fun _ -> Some ()) in
  let print d =
    print_s
      [%message
        ""
          ~is_determined:(Deferred.is_determined d : bool)
          ~num_subscribers:(Bus.num_subscribers bus : int)]
  in
  print d;
  [%expect
    {|
    ((is_determined   false)
     (num_subscribers 1))
    |}];
  Bus.write bus 0;
  print d;
  [%expect
    {|
    ((is_determined   true)
     (num_subscribers 0))
    |}];
  let d = first_exn bus [%here] Arity1 ~f:(fun i -> if i = 13 then Some () else None) in
  Bus.write bus 12;
  print d;
  [%expect
    {|
    ((is_determined   false)
     (num_subscribers 1))
    |}];
  Bus.write bus 13;
  print d;
  [%expect
    {|
    ((is_determined   true)
     (num_subscribers 0))
    |}];
  return ()
;;

let%expect_test "[first_exn] where [~f] raises" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Allow
      ~on_callback_raise:Error.raise
  in
  let d =
    Monitor.try_with_or_error ~rest:`Log (fun () ->
      first_exn bus [%here] Arity1 ~f:(fun _ -> failwith "raising"))
  in
  Bus.write bus 0;
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  print_s ~hide_positions:true [%sexp (d : int Or_error.t Deferred.t)];
  [%expect
    {|
    (Full (
      Error (
        monitor.ml.Error
        ("Bus subscriber raised"
          (exn (Failure raising))
          (backtrace ("<backtrace elided in test>"))
          (subscriber (
            Bus.Subscriber.t (
              (on_callback_raise <fun>)
              (subscribed_from lib/async_bus/test/test_async_bus.ml:LINE:COL)))))
        ("Caught by monitor try_with_or_error"))))
    |}];
  return ()
;;

let%expect_test "[first_exn ~stop:(Deferred.never ())]" =
  (* Providing the [stop] argument tickles some different codepaths.  Check that
     basic functionality still works. *)
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Allow
      ~on_callback_raise:Error.raise
  in
  let d = first_exn ~stop:(Deferred.never ()) bus [%here] Arity1 ~f:Fn.id in
  let print () =
    print_s
      [%message
        ""
          ~is_determined:(Deferred.is_determined d : bool)
          ~num_subscribers:(Bus.num_subscribers bus : int)]
  in
  Bus.write bus None;
  print ();
  [%expect
    {|
    ((is_determined   false)
     (num_subscribers 1))
    |}];
  Bus.write bus (Some 5);
  print ();
  [%expect
    {|
    ((is_determined   true)
     (num_subscribers 0))
    |}];
  return ()
;;

let%expect_test "[first_exn ~stop] where [stop] becomes determined" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Allow
      ~on_callback_raise:Error.raise
  in
  let stop = Ivar.create () in
  let num_calls = ref 0 in
  let d =
    first_exn ~stop:(Ivar.read stop) bus [%here] Arity1 ~f:(fun () ->
      incr num_calls;
      None)
  in
  let print () =
    print_s
      [%message
        (num_calls : int ref)
          ~is_determined:(Deferred.is_determined d : bool)
          ~num_subscribers:(Bus.num_subscribers bus : int)]
  in
  upon d Nothing.unreachable_code;
  print ();
  [%expect
    {|
    ((num_calls       0)
     (is_determined   false)
     (num_subscribers 1))
    |}];
  Bus.write bus ();
  print ();
  [%expect
    {|
    ((num_calls       1)
     (is_determined   false)
     (num_subscribers 1))
    |}];
  Ivar.fill_exn stop ();
  (* [stop] is determined, so even if we write, the callback should not be called. *)
  Bus.write bus ();
  print ();
  [%expect
    {|
    ((num_calls       1)
     (is_determined   false)
     (num_subscribers 1))
    |}];
  (* If we allow the handler on the stop deferred to fire, it will unsubscribe. *)
  let%bind () = Scheduler.yield_until_no_jobs_remain () in
  print ();
  [%expect
    {|
    ((num_calls       1)
     (is_determined   false)
     (num_subscribers 0))
    |}];
  return ()
;;

let%expect_test "[first_exn] when [Allow_and_send_last_value] is used" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Allow_and_send_last_value
      ~on_callback_raise:Error.raise
  in
  (* Send a value, so that later [first_exn] calls see this value. *)
  Bus.write bus ();
  let%bind d = first_exn bus [%here] Arity1 ~f:(fun () -> Some ()) in
  print_s [%message "" ~_:(d : unit)];
  [%expect {| () |}];
  return ()
;;

let%expect_test "[pipe1_exn] where the bus is closed" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Allow
      ~on_callback_raise:Error.raise
  in
  let pipe = pipe1_exn bus [%here] in
  Bus.write bus 13;
  Bus.close bus;
  let%bind all = Pipe.read_all pipe in
  print_s [%sexp (all : int Queue.t)];
  [%expect {| (13) |}];
  return ()
;;

let%expect_test "[pipe1_exn] on a closed bus, varying [on_subscription_after_first_write]"
  =
  let%bind () =
    Deferred.List.iter
      ~how:`Sequential
      On_subscription_after_first_write.all
      ~f:(fun on_subscription_after_first_write ->
        let bus =
          Bus.create_exn
            [%here]
            Arity1
            ~on_subscription_after_first_write
            ~on_callback_raise:Error.raise
        in
        Bus.close bus;
        let pipe = pipe1_exn bus [%here] in
        let is_closed = Pipe.is_closed pipe in
        let%map values = Pipe.to_list pipe in
        print_s
          [%message
            (on_subscription_after_first_write : On_subscription_after_first_write.t)
              (is_closed : bool)
              (values : _ list)])
  in
  [%expect
    {|
    ((on_subscription_after_first_write Allow)
     (is_closed                         true)
     (values ()))
    ((on_subscription_after_first_write Allow_and_send_last_value)
     (is_closed true)
     (values ()))
    ((on_subscription_after_first_write Raise)
     (is_closed                         true)
     (values ()))
    |}];
  return ()
;;

let%expect_test "[pipe1_exn]" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Raise
      ~on_callback_raise:Error.raise
  in
  let pipe = pipe1_exn bus [%here] in
  for i = 0 to 10 do
    Bus.write bus i
  done;
  Bus.close bus;
  let%bind values = Pipe.to_list pipe in
  print_s [%message (values : int list)];
  [%expect {| (values (0 1 2 3 4 5 6 7 8 9 10)) |}];
  return ()
;;

let%expect_test "[pipe1_filter_map_exn]" =
  let bus =
    Bus.create_exn
      [%here]
      Arity1
      ~on_subscription_after_first_write:Raise
      ~on_callback_raise:Error.raise
  in
  let pipe =
    pipe1_filter_map_exn bus [%here] ~f:(function
      | i when i % 2 = 0 -> Some i
      | _ -> None)
  in
  for i = 0 to 10 do
    Bus.write bus i
  done;
  Bus.close bus;
  let%bind filtered_values = Pipe.to_list pipe in
  print_s [%message (filtered_values : int list)];
  [%expect {| (filtered_values (0 2 4 6 8 10)) |}];
  return ()
;;

let%expect_test "[pipe2_filter_map_exn]" =
  let stop = Ivar.create () in
  let bus =
    Bus.create_exn
      [%here]
      Arity2
      ~on_subscription_after_first_write:Raise
      ~on_callback_raise:Error.raise
  in
  let pipe =
    pipe2_filter_map_exn bus [%here] ~stop:(Ivar.read stop) ~f:(fun () -> function
      | i when i % 2 = 0 -> Some i
      | _ -> None)
  in
  for i = 0 to 5 do
    Bus.write2 bus () i
  done;
  Ivar.fill_exn stop ();
  let%bind filtered_values = Pipe.to_list pipe in
  print_s [%message (filtered_values : int list)];
  [%expect {| (filtered_values (0 2 4)) |}];
  for i = 6 to 10 do
    Bus.write2 bus () i
  done;
  require (Pipe.is_empty pipe);
  [%expect {| |}];
  return ()
;;
