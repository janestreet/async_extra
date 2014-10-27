open Core.Std
open Async.Std
open Qtest_lib.Std
open Udp

let sock sock () =
  sock >>| fun sock ->
  let addr = Unix.Socket.getsockname sock in
  Unix.Socket.shutdown sock `Both;
  match addr with
  | `Inet (a, p) ->
    (* assert_equal ~cmp:(Pervasives.(<>)) a Unix.Inet_addr.bind_any; *)
    ignore a;
    assert_equal ~cmp:(<>) p 0
  | `Unix u -> failwith u

let tests = [
  "bind any", sock (bind (`Inet (Unix.Inet_addr.bind_any, 0)));
  "bind localhost", sock (bind (`Inet (Unix.Inet_addr.localhost, 0)));
  "bind_any", sock (bind_any ());
]

let with_socks expected sexp_of f =
  let rev_effects = ref [] in
  bind_any ()
  >>= fun sock1 ->
  Monitor.protect ~finally:(fun () -> Fd.close (Socket.fd sock1))
    (fun () ->
       bind_any ()
       >>= fun sock2 ->
       Monitor.protect ~finally:(fun () -> Fd.close (Socket.fd sock2))
         (fun () ->
            let `Inet (_host1, port1), `Inet (_host2, port2) =
              Unix.Socket.getsockname sock1, Unix.Socket.getsockname sock2
            in
            with_timeout (sec 0.001)
              (f ~sock1 ~sock2 ~effect:(fun e -> rev_effects := e :: !rev_effects)
                 ~addr1:(`Inet (Unix.Inet_addr.localhost, port1))
                 ~addr2:(`Inet (Unix.Inet_addr.localhost, port2)))
            >>= function
            | `Timeout | `Result () ->
              Unix.Socket.shutdown sock1 `Both;
              Unix.Socket.shutdown sock2 `Both;
              Unix.close (Socket.fd sock1) >>= fun () ->
              Unix.close (Socket.fd sock2) >>| fun () ->
              let effects = List.rev !rev_effects in
              if not (Pervasives.(=) expected effects)
              then
                failwiths "unexpected effects" (expected, effects)
                  (Tuple.T2.sexp_of_t
                     (List.sexp_of_t sexp_of)
                     (List.sexp_of_t sexp_of))))

let tests =
  tests
  @ [
    "stop smoke", (fun () ->
      let prefix = ["a"; "b"] in
      let suffix = ["c"; "d"] in
      with_socks prefix <:sexp_of< string >>
        (fun ~sock1 ~sock2 ~effect ~addr1:_ ~addr2 ->
           match sendto () with
           | Error nonfatal ->
             Debug.eprints "nonfatal" nonfatal <:sexp_of< Error.t >>;
             Deferred.unit
           | Ok sendto ->
             let stopped = ref false in
             Deferred.all_unit [
               (Deferred.List.iter ~how:`Sequential (prefix @ suffix)
                  ~f:(fun str ->
                    if !stopped then Deferred.unit
                    else
                      sendto (Socket.fd sock1) (Iobuf.of_string str) addr2
                      >>= fun () ->
                      after (Time.Span.of_us 1.)));
               (Monitor.try_with (fun () ->
                  read_loop (Socket.fd sock2) (fun buf ->
                    let str = Iobuf.to_string buf in
                    effect str;
                    if String.equal str (List.last_exn prefix) then
                      (stopped := true;
                       failwith "Stop")))
                >>| function
                | Error _ when !stopped -> ()
                | Ok () -> ()
                | Error e -> raise e);
             ]));
  ]

let with_fsts send expected sexp_of receiver =
  match send with
  | Error e -> eprintf "%s\n" (Error.to_string_hum e); Deferred.unit
  | Ok send ->
    with_socks expected sexp_of (fun ~sock1 ~sock2 ~effect ~addr1:_ ~addr2 ->
      Deferred.List.iter expected
        ~f:(fun (s, _) -> send (Socket.fd sock1) (Iobuf.of_string s) addr2)
      >>= fun () ->
      receiver ~sock2 ~effect)

let with_send_fsts expected sexp_of receiver () =
  with_fsts (sendto ()) expected sexp_of receiver
  >>= fun () ->
  with_fsts
    (sendto_sync ()
     |> Or_error.map ~f:(fun sendto_sync ->
       (fun fd buf addr ->
          match sendto_sync fd buf addr with
          | `Not_ready -> assert false
          | `Ok -> Deferred.unit)))
    expected
    sexp_of
    receiver

let with_hello_goodbye recv result =
  with_send_fsts ["Hello.", 0; "Goodbye!", 0]
    <:sexp_of< string * int >>
    (fun ~sock2 ~(effect : _ -> unit) ->
       let buf = Iobuf.create ~len:100 in
       recv (Socket.fd sock2) buf
       >>= fun res ->
       Iobuf.flip_lo buf;
       effect (result (Iobuf.to_string buf) res);
       Iobuf.reset buf;
       recv (Socket.fd sock2) buf
       >>| fun res ->
       Iobuf.flip_lo buf;
       effect (result (Iobuf.to_string buf) res))

let tests = tests @ [
  "recvfrom_loop",
  with_send_fsts ["a", 0; "bcd", 0; "efghijklmnop", 0]
    <:sexp_of< string * int >>
    (fun ~sock2 ~effect ->
       recvfrom_loop (Socket.fd sock2)
         (fun b _ -> effect (Iobuf.to_string b, 0)));
  "read_loop",
  with_send_fsts ["a", 0; "bcd", 0; "efghijklmnop", 0]
    <:sexp_of< string * int >>
    (fun ~sock2 ~effect ->
       read_loop (Socket.fd sock2)
         (fun b -> effect (Iobuf.to_string b, 0)));

  (* Queue up some packets and check that they're received all at once.  There's a tiny
     element of faith in assuming they'll be queued rather than dropped and that they're
     delivered in order. *)
  "recvmmsg_loop",
  (match recvmmsg_loop with
   | Error err ->
     (fun () ->
        eprintf "%s\n" (Error.to_string_hum err);
        Deferred.unit)
   | Ok recvmmsg_loop ->
     with_send_fsts [ "Welcome", 0; "to", 1; "the", 2; "jungle!", 3
                    ; "You're", 4; "gonna", 5; "burn!", 6
                    ]
       <:sexp_of< string * int >>
       (fun ~sock2 ~effect ->
          recvmmsg_loop (Socket.fd sock2) (fun ?srcs:_ bufs ~count ->
            for i = 0 to count - 1
            do effect (Iobuf.to_string bufs.(i), i)
            done)))
]
