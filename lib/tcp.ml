open Core.Std
open Import

module Host = Unix.Host
module Socket = Unix.Socket

type 'addr where_to_connect =
  { socket_type : 'addr Socket.Type.t;
    address : unit -> 'addr Deferred.t;
  }

let to_host_and_port host port =
  { socket_type = Socket.Type.tcp;
    address =
      (fun () ->
        Unix.Inet_addr.of_string_or_getbyname host
        >>| fun inet_addr ->
        Socket.Address.Inet.create inet_addr ~port);
  }
;;

let to_file file =
  { socket_type = Socket.Type.unix;
    address = fun () -> return (Socket.Address.Unix.create file);
  }
;;

let to_inet_address address =
  { socket_type = Socket.Type.tcp;
    address = fun () -> return address;
  }
;;

let to_unix_address address =
  { socket_type = Socket.Type.unix;
    address = fun () -> return address;
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
  Deferred.create (fun result ->
    let s = create_socket where_to_connect.socket_type in
    close_sock_on_error s (fun () ->
      Socket.connect_interruptible s address ~interrupt)
    >>> function
    | `Ok s -> Ivar.fill result s
    | `Interrupted ->
      don't_wait_for (Unix.close (Socket.fd s));
      let address = Socket.Address.to_string address in
      if Option.is_some (Deferred.peek timeout) then
        failwiths "connection attempt timeout" address <:sexp_of< string >>
      else
        failwiths "connection attempt aborted" address <:sexp_of< string >>)
;;

type 'a with_connect_options =
     ?buffer_age_limit:[ `At_most of Time.Span.t | `Unlimited ]
  -> ?interrupt:unit Deferred.t
  -> ?reader_buffer_size:int
  -> ?timeout: Time.Span.t
  -> 'a

let connect ?buffer_age_limit ?interrupt ?reader_buffer_size ?timeout where_to_connect =
  connect_sock ?interrupt ?timeout where_to_connect
  >>| fun s ->
  let r, w = reader_writer_of_sock ?buffer_age_limit ?reader_buffer_size s in
  s, r, w
;;

let collect_errors writer f =
  let monitor = Writer.monitor writer in
  ignore (Monitor.errors monitor); (* don't propagate errors up, we handle them here *)
  choose [
    choice (Monitor.error monitor) (fun e -> Error e);
    choice (try_with ~name:"Tcp.collect_errors" f) Fn.id;
  ]
;;

let close_connection r w =
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
  close_connection r w
  >>= fun () ->
  res >>| function
  | Ok v -> v
  | Error e -> raise e
;;

(* We redefine [connect_sock] to cut out optional arguments. *)
let connect_sock where_to_connect = connect_sock where_to_connect

module Where_to_listen = struct

  type ('address, 'listening_on) t =
    { socket_type : 'address Socket.Type.t;
      address : 'address;
      listening_on : 'address -> 'listening_on;
    }

  type inet = (Socket.Address.Inet.t, int   ) t
  type unix = (Socket.Address.Unix.t, string) t

  let create ~socket_type ~address ~listening_on =
    { socket_type; address; listening_on }
  ;;
end

let on_port port =
  { Where_to_listen.
    socket_type          = Socket.Type.tcp;
    address              = Socket.Address.Inet.create_bind_any ~port;
    listening_on         = function `Inet (_, port) -> port;
  }
;;

let on_port_chosen_by_os = on_port 0
;;

let on_file path =
  { Where_to_listen.
    socket_type          = Socket.Type.unix;
    address              = Socket.Address.Unix.create path;
    listening_on         = fun _ -> path;
  }
;;

module Server = struct

  type ('address, 'listening_on)  t =
    { socket : ([ `Passive ], 'address) Socket.t;
      listening_on : 'listening_on;
      buffer_age_limit : Writer.buffer_age_limit option;
      on_handler_error : [ `Raise
                         | `Ignore
                         | `Call of ('address -> exn -> unit)
                         ];
      handle_client : 'address -> Reader.t -> Writer.t -> unit Deferred.t;
      max_connections : int;
      mutable num_connections : int;
      mutable accept_is_pending : bool;
    }
  with fields, sexp_of

  let listening_socket = socket

  type inet = (Socket.Address.Inet.t, int) t
  type unix = (Socket.Address.Unix.t, string) t

  let invariant t : unit =
    try
      let check f field = f (Field.get field t) in
      Fields.iter
        ~socket:ignore
        ~listening_on:ignore
        ~buffer_age_limit:ignore
        ~on_handler_error:ignore
        ~handle_client:ignore
        ~max_connections:(check (fun max_connections ->
          assert (max_connections >= 1)))
        ~num_connections:(check (fun num_connections ->
          assert (num_connections >= 0);
          assert (num_connections <= t.max_connections)))
        ~accept_is_pending:ignore
    with exn ->
      failwiths "invariant failed" (exn, t) <:sexp_of< exn * (_, _) t >>
  ;;

  let fd t = Socket.fd t.socket

  let is_closed      t = Fd.is_closed      (fd t)
  let close_finished t = Fd.close_finished (fd t)
  let close          t = Fd.close          (fd t)

  (* [maybe_accept] is a bit tricky, but the idea is to avoid calling [accept] until we
     have an available slot (determined by [num_connections < max_connections]). *)
  let rec maybe_accept t =
    if   not (is_closed t)
      && t.num_connections < t.max_connections
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
        if is_closed t then
          don't_wait_for (Fd.close (Socket.fd client_socket))
        else begin
          t.num_connections <- t.num_connections + 1;
          (* immediately recurse to try to accept another client *)
          maybe_accept t;
          (* in parallel, handle this client *)
          let r, w =
            reader_writer_of_sock ?buffer_age_limit:t.buffer_age_limit client_socket
          in
          collect_errors w (fun () -> t.handle_client client_address r w)
          >>> fun res ->
          close_connection r w
          >>> fun () ->
          begin match res with
          | Ok ()   -> ()
          | Error e ->
            begin try
              begin match t.on_handler_error with
              | `Ignore -> ()
              | `Raise  -> raise e
              | `Call f -> f client_address e
              end
            with
            | e ->
              don't_wait_for (close t);
              raise e
            end
          end;
          t.num_connections <- t.num_connections - 1;
          maybe_accept t;
        end
    end
  ;;

  let create
      ?(max_connections = 10_000)
      ?max_pending_connections
      ?buffer_age_limit
      ?(on_handler_error = `Raise)
      where_to_listen
      handle_client =
    if max_connections <= 0 then
      Error.failwiths "Tcp.Server.creater got negative [max_connections]" max_connections
        sexp_of_int;
    let module W = Where_to_listen in
    let socket = create_socket where_to_listen.W.socket_type in
    close_sock_on_error socket (fun () ->
      Socket.setopt socket Socket.Opt.reuseaddr true;
      Socket.bind socket where_to_listen.W.address
      >>| Socket.listen ?max_pending_connections)
    >>| fun socket ->
    let t =
      { socket;
        listening_on      = where_to_listen.W.listening_on (Socket.getsockname socket);
        buffer_age_limit;
        on_handler_error;
        handle_client;
        max_connections;
        num_connections   = 0;
        accept_is_pending = false;
      }
    in
    maybe_accept t;
    t
  ;;

  TEST_UNIT "multiclose" =
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

end
