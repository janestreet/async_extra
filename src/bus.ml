open! Core.Std
open! Import

module Bus = Core_kernel.Bus
include Bus

let pipe1_exn (t : ('a -> unit) Read_only.t) =
  let r, w = Pipe.create () in
  let subscription =
    subscribe_exn t [%here] ~f:(function v ->
      Pipe.write_without_pushback_if_open w v)
  in
  upon (Pipe.closed w) (fun () -> unsubscribe t subscription);
  r
;;

module First_arity = struct
  type (_, _, _) t =
    | Arity1 : ('a ->                   unit, 'a ->                   'r option, 'r) t
    | Arity2 : ('a -> 'b ->             unit, 'a -> 'b ->             'r option, 'r) t
    | Arity3 : ('a -> 'b -> 'c ->       unit, 'a -> 'b -> 'c ->       'r option, 'r) t
    | Arity4 : ('a -> 'b -> 'c -> 'd -> unit, 'a -> 'b -> 'c -> 'd -> 'r option, 'r) t
  [@@deriving sexp_of]
end

let first_exn (type c) (type f) (type r)
      t here (first_arity : (c, f, r) First_arity.t) ~(f : f) =
  let module A = First_arity in
  Deferred.create (fun ivar ->
    let subscriber : c Bus.Subscriber.t option ref = ref None in
    let finish : r option -> unit = function
      | None -> ()
      | Some r ->
        Ivar.fill ivar r;
        Bus.unsubscribe t (Option.value_exn !subscriber);
    in
    subscriber :=
      Some
        (Bus.subscribe_exn t here
           ~on_callback_raise:(let monitor = Monitor.current () in
                               fun error -> Monitor.send_exn monitor (Error.to_exn error))
           ~f:(match first_arity with
             | A.Arity1 -> (fun a           -> finish (f a)          )
             | A.Arity2 -> (fun a1 a2       -> finish (f a1 a2)      )
             | A.Arity3 -> (fun a1 a2 a3    -> finish (f a1 a2 a3)   )
             | A.Arity4 -> (fun a1 a2 a3 a4 -> finish (f a1 a2 a3 a4)))))
;;

let%test_unit "first_exn" =
  let bus =
    Bus.create [%here]
      Arity1
      ~allow_subscription_after_first_write:true
      ~on_callback_raise:Error.raise
  in
  let d = first_exn (Bus.read_only bus) [%here] Arity1 ~f:(fun _ -> Some ()) in
  [%test_result: int] (Bus.num_subscribers bus) ~expect:1;
  Bus.write bus 0;
  assert (Deferred.is_determined d);
  [%test_result: int] (Bus.num_subscribers bus) ~expect:0;
  let d =
    first_exn (Bus.read_only bus) [%here] Arity1
      ~f:(fun i -> if i = 13 then Some () else None)
  in
  Bus.write bus 12;
  assert (not (Deferred.is_determined d));
  [%test_result: int] (Bus.num_subscribers bus) ~expect:1;
  Bus.write bus 13;
  assert (Deferred.is_determined d)
;;

let%test_unit "first_exn ~f raises" =
  let bus =
    Bus.create [%here]
      Arity1
      ~allow_subscription_after_first_write:true
      ~on_callback_raise:Error.raise
  in
  let monitor = Monitor.create () in
  Monitor.detach monitor;
  let d =
    within' ~monitor (fun () ->
      first_exn (Bus.read_only bus) [%here] Arity1 ~f:(fun _ -> failwith ""))
  in
  Bus.write bus 0;
  assert (not (Deferred.is_determined d));
  assert (Monitor.has_seen_error monitor);
;;
