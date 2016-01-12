open Core.Std
open Import

module Host = Unix.Host
module Socket = Unix.Socket

type 'addr where_to_connect =
  { socket_type         : 'addr Socket.Type.t
  ; address             : unit -> 'addr Deferred.t
  ; via_local_interface : 'addr option
  }

let to_host_and_port ?via_local_interface host port =
  { socket_type = Socket.Type.tcp
  ; address     =
      (fun () ->
         Unix.Inet_addr.of_string_or_getbyname host
         >>| fun inet_addr->
         Socket.Address.Inet.create inet_addr ~port)
  ; via_local_interface =
      Option.map via_local_interface ~f:(Socket.Address.Inet.create ~port:0)
  }
;;

let to_file file =
  { socket_type = Socket.Type.unix
  ; address     = (fun () -> return (Socket.Address.Unix.create file))
  ; via_local_interface = None
  }
;;

let to_inet_address ?via_local_interface address =
  { socket_type = Socket.Type.tcp
  ; address     = (fun () -> return address)
  ; via_local_interface =
      Option.map via_local_interface ~f:(Socket.Address.Inet.create ~port:0)
  }
;;

let to_unix_address address =
  { socket_type = Socket.Type.unix
  ; address     = (fun () -> return address)
  ; via_local_interface = None
  }
;;

let create_socket socket_type =
  let s = Socket.create socket_type in
  Unix.set_close_on_exec (Unix.Socket.fd s);
  s
;;

let close_sock_on_error s f =
  try_with ~name:"Tcp.close_sock_on_error" f
  >>| function
  | Ok v -> v
  | Error e ->
    (* [close] may fail, but we don't really care, since it will fail
       asynchronously.  The error we really care about is [e], and the
       [raise_error] will cause the current monitor to see that. *)
    don't_wait_for (Unix.close (Socket.fd s));
    raise e;
;;

let reader_writer_of_sock ?buffer_age_limit ?reader_buffer_size s =
  let fd = Socket.fd s in
  (Reader.create ?buf_len:reader_buffer_size fd,
   Writer.create ?buffer_age_limit fd)
;;

let connect_sock ?interrupt ?(timeout = sec 10.) where_to_connect =
  where_to_connect.address ()
  >>= fun address ->
  let timeout = after timeout in
  let interrupt =
    match interrupt with
    | None           -> timeout
    | Some interrupt -> Deferred.any [ interrupt; timeout ]
  in
  let connect_interruptible s =
    Socket.connect_interruptible s address ~interrupt
  in
  Deferred.create (fun result ->
    let s = create_socket where_to_connect.socket_type in
    close_sock_on_error s (fun () ->
      match where_to_connect.via_local_interface with
      | None -> connect_interruptible s
      | Some local_interface ->
        Socket.bind s local_interface
        >>= fun s ->
        connect_interruptible s)
    >>> function
    | `Ok s -> Ivar.fill result s
    | `Interrupted ->
      don't_wait_for (Unix.close (Socket.fd s));
      let address = Socket.Address.to_string address in
      if Option.is_some (Deferred.peek timeout)
      then failwiths "connection attempt timeout" address [%sexp_of: string]
      else failwiths "connection attempt aborted" address [%sexp_of: string])
;;

type 'a with_connect_options
  =  ?buffer_age_limit   : [ `At_most of Time.Span.t | `Unlimited ]
  -> ?interrupt          : unit Deferred.t
  -> ?reader_buffer_size : int
  -> ?timeout            : Time.Span.t
  -> 'a

let connect ?buffer_age_limit ?interrupt ?reader_buffer_size ?timeout where_to_connect =
  connect_sock ?interrupt ?timeout where_to_connect
  >>| fun s ->
  let r, w = reader_writer_of_sock ?buffer_age_limit ?reader_buffer_size s in
  s, r, w
;;

let collect_errors writer f =
  let monitor = Writer.monitor writer in
  ignore (Monitor.detach_and_get_error_stream monitor); (* don't propagate errors up, we handle them here *)
  choose [
    choice (Monitor.get_next_error monitor) (fun e -> Error e);
    choice (try_with ~name:"Tcp.collect_errors" f) Fn.id;
  ]
;;

let close_connection_via_reader_and_writer r w =
  Writer.close w ~force_close:(Clock.after (sec 30.))
  >>= fun () ->
  Reader.close r
;;

let with_connection
      ?buffer_age_limit ?interrupt ?reader_buffer_size ?timeout
      where_to_connect f =
  connect_sock ?interrupt ?timeout where_to_connect
  >>= fun socket ->
  let r, w = reader_writer_of_sock ?buffer_age_limit ?reader_buffer_size socket in
  let res = collect_errors w (fun () -> f socket r w) in
  Deferred.any [
    res >>| (fun (_ : ('a, exn) Result.t) -> ());
    Reader.close_finished r;
    Writer.close_finished w;
  ]
  >>= fun () ->
  close_connection_via_reader_and_writer r w
  >>= fun () ->
  res >>| function
  | Ok v -> v
  | Error e -> raise e
;;

module Where_to_listen = struct

  type ('address, 'listening_on) t =
    { socket_type  : 'address Socket.Type.t
    ; address      : 'address
    ; listening_on : ('address -> 'listening_on) sexp_opaque
    }
  [@@deriving sexp_of, fields]

  type inet = (Socket.Address.Inet.t, int   ) t [@@deriving sexp_of]
  type unix = (Socket.Address.Unix.t, string) t [@@deriving sexp_of]

  let create ~socket_type ~address ~listening_on =
    { socket_type; address; listening_on }
  ;;
end

let on_port port =
  { Where_to_listen.
    socket_type  = Socket.Type.tcp
  ; address      = Socket.Address.Inet.create_bind_any ~port
  ; listening_on = function `Inet (_, port) -> port
  }
;;

let on_port_chosen_by_os = on_port 0
;;

let on_file path =
  { Where_to_listen.
    socket_type  = Socket.Type.unix
  ; address      = Socket.Address.Unix.create path
  ; listening_on = fun _ -> path
  }
;;

module Server = struct

  module Connection = struct
    type 'address t =
      { client_socket  : ([ `Active ], 'address) Socket.t
      ; client_address : 'address
      }
    [@@deriving fields, sexp_of]

    let invariant invariant_address t =
      Invariant.invariant [%here] t [%sexp_of: _ t] (fun () ->
        let check f = Invariant.check_field t f in
        Fields.iter
          ~client_socket:ignore
          ~client_address:(check invariant_address))
    ;;

    let create ~client_socket ~client_address = { client_socket; client_address }

    let close t = Fd.close (Socket.fd t.client_socket)
  end

  type ('address, 'listening_on)  t =
    { socket                    : ([ `Passive ], 'address) Socket.t
    ; listening_on              : 'listening_on
    ; on_handler_error          : [ `Raise
                                  | `Ignore
                                  | `Call of ('address -> exn -> unit)
                                  ]
    ; handle_client             : 'address
                                -> ([ `Active ], 'address) Socket.t
                                -> (unit, exn) Result.t Deferred.t
    ; max_connections           : int
    ; connections               : 'address Connection.t Bag.t
    ; mutable accept_is_pending : bool
    }
  [@@deriving fields, sexp_of]

  let num_connections t = Bag.length t.connections

  type ('address, 'listening_on, 'callback) create_options
    =  ?max_connections  : int
    -> ?backlog          : int
    -> ?on_handler_error : [ `Raise
                           | `Ignore
                           | `Call of ('address -> exn -> unit)
                           ]
    -> ('address, 'listening_on) Where_to_listen.t
    -> 'callback
    -> ('address, 'listening_on) t Deferred.t

  let listening_socket = socket

  type inet = (Socket.Address.Inet.t, int)    t [@@deriving sexp_of]
  type unix = (Socket.Address.Unix.t, string) t [@@deriving sexp_of]

  let listening_on_address (t : (_, _) t) = Socket.getsockname t.socket

  let invariant t : unit =
    try
      let check f field = f (Field.get field t) in
      Fields.iter
        ~socket:ignore
        ~listening_on:ignore
        ~on_handler_error:ignore
        ~handle_client:ignore
        ~max_connections:(check (fun max_connections ->
          assert (max_connections >= 1)))
        ~connections:(check (fun connections ->
          Bag.invariant (Connection.invariant ignore) connections;
          let num_connections = num_connections t in
          assert (num_connections >= 0);
          assert (num_connections <= t.max_connections)))
        ~accept_is_pending:ignore
    with exn ->
      failwiths "invariant failed" (exn, t) [%sexp_of: exn * (_, _) t]
  ;;

  let fd t = Socket.fd t.socket

  let is_closed      t = Fd.is_closed      (fd t)
  let close_finished t = Fd.close_finished (fd t)

  let close ?(close_existing_connections = false) t =
    let fd_closed = Fd.close (fd t) in
    if not close_existing_connections
    then fd_closed
    else
      (* Connections are removed from the bag by the [maybe_accept] below, as the fds are
         closed. *)
      Deferred.all_unit (fd_closed
                         :: List.map (Bag.to_list t.connections) ~f:Connection.close)
  ;;

  (* [maybe_accept] is a bit tricky, but the idea is to avoid calling [accept] until we
     have an available slot (determined by [num_connections < max_connections]). *)
  let rec maybe_accept t =
    if not (is_closed t)
    && num_connections t < t.max_connections
    && not t.accept_is_pending
    then begin
      t.accept_is_pending <- true;
      Socket.accept t.socket
      >>> fun accept_result ->
      t.accept_is_pending <- false;
      match accept_result with
      | `Socket_closed -> ()
      | `Ok (client_socket, client_address) ->
        (* It is possible that someone called [close t] after the [accept] returned but
           before we got here.  In that case, we just close the client. *)
        if is_closed t
        then don't_wait_for (Fd.close (Socket.fd client_socket))
        else begin
          let connection = Connection.create ~client_socket ~client_address in
          let connections_elt = Bag.add t.connections connection in
          (* immediately recurse to try to accept another client *)
          maybe_accept t;
          (* in parallel, handle this client *)
          t.handle_client client_address client_socket
          >>> fun res ->
          Connection.close connection
          >>> fun () ->
          begin match res with
          | Ok ()   -> ()
          | Error e ->
            try
              match t.on_handler_error with
              | `Ignore -> ()
              | `Raise  -> raise e
              | `Call f -> f client_address e
            with
            | e ->
              don't_wait_for (close t);
              raise e
          end;
          Bag.remove t.connections connections_elt;
          maybe_accept t;
        end
    end
  ;;

  let create_sock_internal
        ?(max_connections = 10_000)
        ?backlog
        ?(on_handler_error = `Raise)
        (where_to_listen : _ Where_to_listen.t)
        handle_client =
    if max_connections <= 0
    then failwiths "Tcp.Server.creater got negative [max_connections]" max_connections
           sexp_of_int;
    let socket = create_socket where_to_listen.socket_type in
    close_sock_on_error socket (fun () ->
      Socket.setopt socket Socket.Opt.reuseaddr true;
      Socket.bind socket where_to_listen.address
      >>| Socket.listen ?backlog)
    >>| fun socket ->
    let t =
      { socket
      ; listening_on      = where_to_listen.listening_on (Socket.getsockname socket)
      ; on_handler_error
      ; handle_client
      ; max_connections
      ; connections       = Bag.create ()
      ; accept_is_pending = false
      }
    in
    maybe_accept t;
    t
  ;;

  let create_sock
        ?max_connections
        ?backlog
        ?on_handler_error
        where_to_listen
        handle_client =
    create_sock_internal
      ?max_connections
      ?backlog
      ?on_handler_error
      where_to_listen
      (fun client_address client_socket ->
         try_with ~name:"Tcp.Server.create_sock"
           (fun () -> handle_client client_address client_socket))
  ;;

  let create
        ?buffer_age_limit
        ?max_connections
        ?backlog
        ?on_handler_error
        where_to_listen
        handle_client =
    create_sock_internal
      ?max_connections
      ?backlog
      ?on_handler_error
      where_to_listen
      (fun client_address client_socket ->
         let r, w = reader_writer_of_sock ?buffer_age_limit client_socket in
         collect_errors w (fun () -> handle_client client_address r w)
         >>= fun res ->
         close_connection_via_reader_and_writer r w
         >>| fun () ->
         res)
  ;;

  let%test_unit "multiclose" =
    Thread_safe.block_on_async_exn (fun () ->
      let units n = List.range 0 n |> List.map ~f:ignore in
      let multi how f = Deferred.List.iter ~how ~f (units 10) in
      let close_connection r w () = Reader.close r >>= fun () -> Writer.close w in
      let multi_close_connection r w () = multi `Parallel (close_connection r w) in
      create on_port_chosen_by_os (fun _address r w -> multi_close_connection r w ())
      >>= fun t ->
      multi `Parallel (fun () ->
        with_connection (to_host_and_port "localhost" t.listening_on)
          (fun _socket r w -> multi_close_connection r w ()))
      >>= fun () ->
      multi `Sequential (fun () ->
        multi `Parallel (fun () -> close t)
        >>= fun () ->
        multi `Parallel (fun () -> Fd.close (Socket.fd t.socket))))
  ;;

  let%test_unit "establish at least max_connections + backlog" =
    let test_connect address expect =
      let test = [%test_result:[ `Accepted | `Rejected ]] ~expect in
      try_with ~extract_exn:true (fun () -> connect (to_inet_address address))
      >>= function
      | Error (Unix.Unix_error (ECONNREFUSED, _, _)) -> return (test `Rejected)
      | Error e -> failwiths "connect" e [%sexp_of: exn]
      | Ok (server, r, w) ->
        Monitor.protect ~finally:(fun () -> close_connection_via_reader_and_writer r w)
          (fun () ->
            match
              Unix.Syscall_result.Unit.to_result
                (Fd.with_file_descr_exn (Socket.fd server)
                   (Iobuf.read_assume_fd_is_nonblocking (Iobuf.create ~len:1)))
            with
            | Error (EAGAIN | EWOULDBLOCK) -> return (test `Accepted)
            | r -> failwiths "read" r [%sexp_of: (unit, Unix.Error.t) Result.t])
    in
    (* We use a small [backlog] to keep the number of simultaneous threads small, to
       avoid spurious hydra rejects due to Async outputting on stderr complaints about
       the thread pool being stuck. *)
    List.iter [ 10; 3; 2; 1; 0 ] ~f:(fun backlog ->
      Thread_safe.block_on_async_exn
        (fun () ->
           try_with
             (fun () ->
                let max_connections = 1 in
                create ~max_connections ~backlog on_port_chosen_by_os
                  (* [never ()] keeps all the connections open. *)
                  (fun _ _ _ -> never ())
                >>= fun t ->
                let address = Socket.getsockname (listening_socket t) in
                (* [address] is a 0.0.0.0 address but it still seems to work with
                   [connect], i.e., it does connect to the server.  Ordinarily, one would
                   replace the address with 127.0.0.1, but that does not seem to be
                   necessary. *)
                Monitor.protect ~finally:(fun () -> close t)
                  (fun () ->
                     Deferred.List.iter ~how:`Parallel
                       (List.range 0 (max_connections + backlog))
                       ~f:(fun i ->
                         (* These [`Accepted] cases are not expected to fail unless
                            [backlog] is capped.  In Linux, for example, this can happen
                            if [tcp_max_syn_backlog] is reduced below 128 (from the
                            default 2048, typically).

                            Other vagaries of [backlog] lead to more connections being
                            accepted rather than fewer. *)
                         try_with (fun () -> test_connect address `Accepted)
                         >>| function
                         | Ok () -> ()
                         | Error e -> failwiths "connection" (i, e)
                                        [%sexp_of: int * exn])
                     >>= fun () ->
                     (* As now documented ad nauseum, we can't be sure excess connections
                        will be actively rejected, so don't check. *)
                     if false
                     then test_connect address `Rejected
                     else Deferred.unit))
           >>| function
           | Ok () -> ()
           | Error e -> failwiths "backlog" (backlog, e) [%sexp_of: int * exn]))
  ;;

  (* This tests that setting SO_LINGER to 0 on a server's connection to a client and then
     closing causes the client to receive an RST rather than an orderly shutdown. *)
  let%test_module "SO_LINGER" =
    (module struct
      let test ~linger ~expect =
        Thread_safe.block_on_async_exn
          (fun () ->
             let backlog = 10 in
             create ~backlog on_port_chosen_by_os
               (fun _ _ to_client ->
                  let to_client = Writer.fd to_client in
                  if not linger then
                    Fd.with_file_descr_exn to_client
                      (fun file_descr ->
                         Core.Std.Unix.setsockopt_optint file_descr SO_LINGER (Some 0));
                  Fd.close to_client)
             >>= fun t ->
             Deferred.all (List.init backlog ~f:(fun _ ->
               connect (to_inet_address (Socket.getsockname (listening_socket t)))
               >>= fun (connection_to_server, _, _) ->
               let connection_to_server = Socket.fd connection_to_server in
               after (sec 0.1)
               >>= fun () ->
               let result =
                 match
                   Fd.with_file_descr connection_to_server
                     (fun file_descr ->
                        Unix.Error.of_system_int
                          ~errno:(Core.Std.Unix.getsockopt_int file_descr SO_ERROR))
                 with
                 | `Ok e when e = expect -> Ok ()
                 | wrong ->
                   Error (wrong |> [%sexp_of: [ `Already_closed
                                              | `Error of exn
                                              | `Ok of Unix.Error.t
                                              ]])
               in
               Fd.close connection_to_server
               >>= fun () ->
               return result))
             >>= fun results ->
             close t
             >>| fun () ->
             if not (List.for_all results ~f:is_ok)
             then failwiths "results" (results, t)
                    [%sexp_of: (unit, Sexp.t) Result.t list * inet])
      ;;

      let%test_unit "setting to zero and closing connection sends RST" =
        (* This test fails rarely but nondeterministically.  Rather than tweaking the
           timing back and forth, since there's an inherent race, let's just disable the
           test but leave it in place no run manually whenever we want. *)
        if false then test ~linger:false ~expect:EPIPE (* apparently not ECONNRESET *)
      ;;

      let%test_unit "control: no RST without setting to zero" =
        test ~linger:true ~expect:(EUNKNOWNERR 0)
      ;;
    end)
end
