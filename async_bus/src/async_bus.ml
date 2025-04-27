open! Core
open! Async_kernel
open! Import
open! Bus

let subscribe_and_maybe_write_to_pipe ?stop ~(here : [%call_pos]) t ~maybe_write_fn =
  if Bus.is_closed t
  then Pipe.empty ()
  else (
    let r, w = Pipe.create () in
    let subscription =
      subscribe_exn t ~here ~f:(maybe_write_fn w) ~on_close:(fun () -> Pipe.close w)
    in
    (match stop with
     | None -> ()
     | Some stop -> upon stop (fun () -> Pipe.close w));
    upon (Pipe.closed w) (fun () -> unsubscribe t subscription);
    r)
;;

let pipe1_exn ~(here : [%call_pos]) t =
  subscribe_and_maybe_write_to_pipe
    t
    ~here
    ~maybe_write_fn:Pipe.write_without_pushback_if_open
;;

let pipe1_filter_map_exn ~(here : [%call_pos]) t ~f =
  subscribe_and_maybe_write_to_pipe t ~here ~maybe_write_fn:(fun pipe v ->
    match f v with
    | None -> ()
    | Some v -> Pipe.write_without_pushback_if_open pipe v)
;;

let pipe2_filter_map_exn ?stop ~(here : [%call_pos]) t ~f =
  subscribe_and_maybe_write_to_pipe ?stop t ~here ~maybe_write_fn:(fun pipe a b ->
    match f a b with
    | None -> ()
    | Some v -> Pipe.write_without_pushback_if_open pipe v)
;;

module First_arity = struct
  type (_, _, _) t =
    | Arity1 : ('a -> unit, 'a -> 'r option, 'r) t
    | Arity1_local : (local_ 'a -> unit, local_ 'a -> 'r option, 'r) t
    | Arity2 : ('a -> 'b -> unit, 'a -> 'b -> 'r option, 'r) t
    | Arity2_local :
        (local_ 'a -> local_ 'b -> unit, local_ 'a -> local_ 'b -> 'r option, 'r) t
    | Arity3 : ('a -> 'b -> 'c -> unit, 'a -> 'b -> 'c -> 'r option, 'r) t
    | Arity4 : ('a -> 'b -> 'c -> 'd -> unit, 'a -> 'b -> 'c -> 'd -> 'r option, 'r) t
    | Arity5 :
        ( 'a -> 'b -> 'c -> 'd -> 'e -> unit
          , 'a -> 'b -> 'c -> 'd -> 'e -> 'r option
          , 'r )
          t
  [@@deriving sexp_of]
end

let first_exn
  (type c f r)
  ?stop
  ~(here : [%call_pos])
  t
  (first_arity : (c, f, r) First_arity.t)
  ~(f : f)
  =
  Deferred.create (fun ivar ->
    let subscriber : c Bus.Subscriber.t option ref = ref None in
    let finish : r option -> unit = function
      | None -> ()
      | Some r ->
        Ivar.fill_exn ivar r;
        (match !subscriber with
         | Some subscriber -> Bus.unsubscribe t subscriber
         | None ->
           (* When a [Bus] is created with
              [on_subscription_after_first_write:Allow_and_send_last_value], then [finish]
              can be called before the [Bus.subscribe_exn] below returns.  In that case,
              we won't have captured the subscriber yet.  Instead of [Option.value_exn
              !subscriber], match here and check again after [subscribe_exn] returns. *)
           ())
    in
    (* We define [can_finish] separately from [finish] because we must call [can_finish]
       before we call [f], so that we do not call [f] if [stop] is determined. *)
    let can_finish =
      match stop with
      | None -> fun () -> true
      | Some stop ->
        upon stop (fun () -> Bus.unsubscribe t (Option.value_exn !subscriber));
        fun () -> not (Deferred.is_determined stop)
    in
    let callback : c =
      match first_arity with
      | Arity1 -> fun a -> if can_finish () then finish (f a)
      | Arity1_local -> fun a -> if can_finish () then finish (f a)
      | Arity2 -> fun a1 a2 -> if can_finish () then finish (f a1 a2)
      | Arity2_local -> fun a1 a2 -> if can_finish () then finish (f a1 a2)
      | Arity3 -> fun a1 a2 a3 -> if can_finish () then finish (f a1 a2 a3)
      | Arity4 -> fun a1 a2 a3 a4 -> if can_finish () then finish (f a1 a2 a3 a4)
      | Arity5 -> fun a1 a2 a3 a4 a5 -> if can_finish () then finish (f a1 a2 a3 a4 a5)
    in
    subscriber
    := Some
         (Bus.subscribe_exn
            t
            ~here
            ~on_callback_raise:
              (let monitor = Monitor.current () in
               fun error -> Monitor.send_exn monitor (Error.to_exn error))
            ~f:callback);
    if Ivar.is_full ivar then Bus.unsubscribe t (Option.value_exn !subscriber))
;;
