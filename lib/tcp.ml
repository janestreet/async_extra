open Core.Std
open Import

module Host = Unix.Host
module Socket = Unix.Socket

let create_socket sock_type =
  let s = Socket.create sock_type in
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
      whenever (Unix.close (Socket.fd s));
      raise e;
;;

let reader_writer_of_sock ?max_buffer_age ?reader_buffer_size s =
  let fd = Socket.fd s in
  (* If max_buffer_age is not specified, use the default in Writer. *)
  let buffer_age_limit =
    match max_buffer_age with
    | None     -> None
    | Some age -> Some (`At_most age)
  in
  (Reader.create ?buf_len:reader_buffer_size fd,
   Writer.create ?buffer_age_limit fd)
;;

let connect_sock_gen ?interrupt ?(timeout = sec 10.) ~sock_type ~sock_addr () =
  let timeout = after timeout in
  let interrupt =
    match interrupt with
    | None           -> timeout
    | Some interrupt -> choose_ident [ interrupt; timeout ]
  in
  Deferred.create (fun result ->
    let s = create_socket sock_type in
    close_sock_on_error s (fun () ->
      Socket.connect_interruptible s sock_addr ~interrupt)
    >>> function
    | `Ok s -> Ivar.fill result s
    | `Interrupted ->
      whenever (Unix.close (Socket.fd s));
      let sock_addr = Socket.Address.to_string sock_addr in
      if Option.is_some (Deferred.peek timeout) then
        failwiths "connection attempt timeout" sock_addr <:sexp_of< string >>
      else
        failwiths "connection attempt aborted" sock_addr <:sexp_of< string >>)
;;

let connect_sock ?interrupt ?timeout ~host ~port () =
  Unix.Inet_addr.of_string_or_getbyname host >>= fun inet_addr ->
  let sock_addr = Socket.Address.inet inet_addr ~port in
  connect_sock_gen
    ?interrupt
    ?timeout
    ~sock_type:Socket.Type.tcp
    ~sock_addr ()
;;

let connect_sock_unix ?interrupt ?timeout ~file () =
  connect_sock_gen
    ?interrupt ?timeout
    ~sock_type:Socket.Type.unix
    ~sock_addr:(Socket.Address.unix file) ()
;;

let close_connection r w =
  Writer.close w ~force_close:(Clock.after (sec 30.))
  >>= fun () ->
  Reader.close r
;;

let connect ?max_buffer_age ?interrupt ?timeout ?reader_buffer_size ~host ~port () =
  connect_sock ?interrupt ?timeout ~host ~port ()
  >>| fun s ->
  reader_writer_of_sock ?max_buffer_age ?reader_buffer_size s
;;

let connect_unix ?max_buffer_age ?interrupt ?timeout ?reader_buffer_size ~file () =
  connect_sock_unix ?interrupt ?timeout ~file ()
  >>| fun s ->
  reader_writer_of_sock ?max_buffer_age ?reader_buffer_size s
;;

let collect_errors writer f =
  let monitor = Writer.monitor writer in
  ignore (Monitor.errors monitor); (* don't propagate errors up, we handle them here *)
  choose [
    choice (Monitor.error monitor) (fun e -> Error e);
    choice (try_with ~name:"Tcp.collect_errors" f) Fn.id;
  ]
;;

let with_connection ?interrupt ?timeout ?max_buffer_age ~host ~port f =
  connect_sock ?interrupt ?timeout ~host ~port ()
  >>= fun s ->
  let r,w    = reader_writer_of_sock ?max_buffer_age s in
  let res    = collect_errors w (fun () -> f r w) in
  Deferred.choose_ident [
    res >>| (fun (_ : ('a, exn) Result.t) -> ());
    Reader.closed r;
    Writer.close_finished w;
  ]
  >>= fun () ->
  close_connection r w
  >>= fun () ->
  res >>| function
  | Ok v -> v
  | Error e -> raise (Monitor.extract_exn e)
;;

let handle_client ?max_buffer_age s addr f =
  let r, w = reader_writer_of_sock ?max_buffer_age s in
  collect_errors w (fun () -> f addr r w)
  >>= fun res ->
  close_connection r w
  >>| fun () ->
  res
;;

exception Tcp_server_negative_max_connections of int with sexp

let serve_gen ?(max_connections=10_000) ?max_pending_connections ?max_buffer_age
    ~sock_type
    ~sock_addr
    ~on_handler_error handler =
  Deferred.create (fun ready ->
    if max_connections <= 0 then
      raise (Tcp_server_negative_max_connections max_connections);
    let s = create_socket sock_type in
    close_sock_on_error s (fun () ->
      Socket.setopt s Socket.Opt.reuseaddr true;
      Socket.bind s sock_addr
      >>| Socket.listen ?max_pending_connections)
    >>> fun s ->
    Ivar.fill ready ();
    let num_connections   = ref 0 in
    let accept_is_pending = ref false in
    let rec accept_loop () =
      if !num_connections < max_connections && not !accept_is_pending then begin
        accept_is_pending := true;
        Socket.accept s
        >>> fun (client_s, addr) ->
        accept_is_pending := false;
        incr num_connections;
        accept_loop ();
        handle_client ?max_buffer_age client_s addr handler
        >>> fun res ->
        begin match res with
        | Ok () -> ()
        | Error e ->
          match on_handler_error with
          | `Ignore -> ()
          | `Raise  -> raise e
          | `Call f -> f addr e
        end;
        decr num_connections;
        accept_loop ()
      end
    in
    accept_loop ())
;;

let serve ?max_connections ?max_pending_connections ?max_buffer_age ~port
    ~on_handler_error handler =
  serve_gen ?max_connections ?max_pending_connections ?max_buffer_age
    ~sock_type:Socket.Type.tcp
    ~sock_addr:(Socket.Address.inet_addr_any ~port)
    ~on_handler_error
    handler

let serve_unix ?max_connections ?max_pending_connections ?max_buffer_age ~file
    ~on_handler_error handler =
  serve_gen ?max_connections ?max_pending_connections ?max_buffer_age
    ~sock_type:Socket.Type.unix
    ~sock_addr:(Socket.Address.unix file)
    ~on_handler_error
    handler

let connect_sock ~host ~port = connect_sock ~host ~port ()
let connect_sock_unix ~file = connect_sock_unix ~file ()
;;
