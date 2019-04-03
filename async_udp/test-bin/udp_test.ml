open! Core
open! Async
open! Qtest_lib.Std
open! Async_udp

let sock sock () =
  let addr = Unix.Socket.getsockname sock in
  Unix.Socket.shutdown sock `Both;
  match addr with
  | `Inet (a, p) ->
    (* assert_equal ~cmp:(Poly.(<>)) a Unix.Inet_addr.bind_any; *)
    ignore a;
    assert_equal ~cmp:( <> ) p 0;
    return ()
  | `Unix u -> failwith u
;;

let tests =
  [ "bind any", sock (bind (`Inet (Unix.Inet_addr.bind_any, 0)))
  ; "bind localhost", sock (bind (`Inet (Unix.Inet_addr.localhost, 0)))
  ; "bind_any", sock (bind_any ())
  ]
;;

let with_socks ~expected_effects sexp_of_effect f =
  let sock1 = bind_any () in
  Monitor.protect
    ~finally:(fun () -> Fd.close (Socket.fd sock1))
    (fun () ->
       let sock2 = bind_any () in
       Monitor.protect
         ~finally:(fun () -> Fd.close (Socket.fd sock2))
         (fun () ->
            let `Inet (_host1, port1), `Inet (_host2, port2) =
              Unix.Socket.getsockname sock1, Unix.Socket.getsockname sock2
            in
            let rev_effects = ref [] in
            (* This timeout looks long, but with substantial concurrency, as may occur on
               Hydra workers, even 10ms is too short. *)
            with_timeout
              (sec 0.1)
              (f
                 ~sock1
                 ~sock2
                 ~effect:(fun e -> rev_effects := e :: !rev_effects)
                 ~addr1:(`Inet (Unix.Inet_addr.localhost, port1))
                 ~addr2:(`Inet (Unix.Inet_addr.localhost, port2)))
            >>| fun outcome ->
            let effects = List.rev !rev_effects in
            if not (Poly.( = ) expected_effects effects)
            then
              failwiths
                "unexpected effects"
                [%sexp
                  ~~(outcome : [`Result of _ | `Timeout])
                , ~~(effects : effect list)
                , ~~(expected_effects : effect list)]
                [%sexp_of: Sexp.t]))
;;


let with_fsts send ~expected_effects sexp_of_effect receiver =
  match send with
  | Error e ->
    eprintf "%s\n" (Error.to_string_hum e);
    Deferred.unit
  | Ok send ->
    with_socks
      ~expected_effects
      sexp_of_effect
      (fun ~sock1 ~sock2 ~effect ~addr1:_ ~addr2 ->
         Deferred.List.iter expected_effects ~f:(fun (s, _) ->
           send (Socket.fd sock1) (Iobuf.of_string s) addr2)
         >>= fun () -> receiver ~sock2 ~effect)
;;

let with_send_fsts ~expected_effects sexp_of_effect receiver () =
  with_fsts (sendto ()) ~expected_effects sexp_of_effect receiver
  >>= fun () ->
  with_fsts
    (Or_error.map (sendto_sync ()) ~f:(fun sendto_sync fd buf addr ->
       match Unix.Syscall_result.Unit.to_result (sendto_sync fd buf addr) with
       | Ok () -> Deferred.unit
       | Error (EWOULDBLOCK | EAGAIN | EINTR) -> assert false
       | Error e -> raise (Unix.Unix_error (e, "sendto", ""))))
    ~expected_effects
    sexp_of_effect
    receiver
;;

let tests =
  tests
  @ [ ( "recvfrom_loop"
      , with_send_fsts
          ~expected_effects:[ "a", 0; "bcd", 0; "efghijklmnop", 0 ]
          [%sexp_of: string * int]
          (fun ~sock2 ~effect ->
             recvfrom_loop (Socket.fd sock2) (fun b _ -> effect (Iobuf.to_string b, 0))) )
    ; ( "read_loop"
      , with_send_fsts
          ~expected_effects:[ "a", 0; "bcd", 0; "efghijklmnop", 0 ]
          [%sexp_of: string * int]
          (fun ~sock2 ~effect ->
             read_loop (Socket.fd sock2) (fun b -> effect (Iobuf.to_string b, 0))) )
      ; (* Queue up some packets and check that they're received all at once.  There's a tiny
           element of faith in assuming they'll be queued rather than dropped and that they're
           delivered in order. *)
      ( "recvmmsg_loop"
      , match recvmmsg_loop with
      | Error err ->
        fun () ->
          eprintf "%s\n" (Error.to_string_hum err);
          Deferred.unit
      | Ok recvmmsg_loop ->
        with_send_fsts
          ~expected_effects:
            [ "Welcome", 0
            ; "to", 1
            ; "the", 2
            ; "jungle!", 3
            ; "You're", 4
            ; "gonna", 5
            ; "burn!", 6
            ]
          [%sexp_of: string * int]
          (fun ~sock2 ~effect ->
             recvmmsg_loop (Socket.fd sock2) (fun bufs ~count ->
               for i = 0 to count - 1 do
                 effect (Iobuf.to_string bufs.(i), i)
               done)) )
    ]
;;
