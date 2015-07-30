open Core.Std
open Import

module Transport = Rpc_transport
module Low_latency_transport = Rpc_transport_low_latency

module Any             = Rpc_kernel.Any
module Description     = Rpc_kernel.Description
module Implementation  = Rpc_kernel.Implementation
module Implementations = Rpc_kernel.Implementations
module One_way         = Rpc_kernel.One_way
module Pipe_rpc        = Rpc_kernel.Pipe_rpc
module Rpc             = Rpc_kernel.Rpc
module State_rpc       = Rpc_kernel.State_rpc

module Connection = struct
  module Heartbeat_config = struct
    open Core.Stable
    module Stable = struct
      module V1 = struct
        type t =
          { timeout    : Span.V2.t
          ; send_every : Span.V2.t
          } with sexp, bin_io
      end
      module V2 = struct
        type t = Rpc_kernel.Connection.Heartbeat_config.t =
          { timeout    : Time_ns.Span.V1.t
          ; send_every : Time_ns.Span.V1.t
          } with sexp, bin_io
      end
    end
    include Stable.V1

    open Core.Std
    let v2_of_v1 (t : Stable.V1.t) : Stable.V2.t =
      { timeout    = Time_ns.Span.of_span t.timeout
      ; send_every = Time_ns.Span.of_span t.send_every
      }
  end

  include (Rpc_kernel.Connection : module type of struct include Rpc_kernel.Connection end
           with module Heartbeat_config := Rpc_kernel.Connection.Heartbeat_config)

  (* unfortunately, copied from reader0.ml *)
  let default_max_message_size = 100 * 1024 * 1024

  let create
        ?implementations
        ~connection_state
        ?(max_message_size=default_max_message_size)
        ?handshake_timeout
        ?heartbeat_config
        ?description
        reader writer =
    create
      ?implementations
      ~connection_state
      ?handshake_timeout:(Option.map handshake_timeout ~f:Time_ns.Span.of_span)
      ?heartbeat_config:(Option.map heartbeat_config ~f:Heartbeat_config.v2_of_v1)
      ?description
      (Transport.of_reader_writer reader writer ~max_message_size)
  ;;

  let with_close
        ?implementations
        ?(max_message_size=default_max_message_size)
        ?handshake_timeout
        ?heartbeat_config
        ~connection_state
        reader writer
        ~dispatch_queries
        ~on_handshake_error
    =
    with_close
      ?implementations
      ?handshake_timeout:(Option.map handshake_timeout ~f:Time_ns.Span.of_span)
      ?heartbeat_config:(Option.map heartbeat_config ~f:Heartbeat_config.v2_of_v1)
      ~connection_state
      (Transport.of_reader_writer reader writer ~max_message_size)
      ~dispatch_queries
      ~on_handshake_error
  ;;

  let server_with_close
        ?(max_message_size=default_max_message_size)
        ?handshake_timeout
        ?heartbeat_config
        reader writer
        ~implementations
        ~connection_state
        ~on_handshake_error
    =
    server_with_close
      ?handshake_timeout:(Option.map handshake_timeout ~f:Time_ns.Span.of_span)
      ?heartbeat_config:(Option.map heartbeat_config ~f:Heartbeat_config.v2_of_v1)
      (Transport.of_reader_writer reader writer ~max_message_size)
      ~implementations
      ~connection_state
      ~on_handshake_error
  ;;

  let collect_errors (transport : Transport.t) ~f =
    let monitor = Transport.Writer.monitor transport.writer in
    (* don't propagate errors up, we handle them here *)
    ignore (Monitor.detach_and_get_error_stream monitor);
    choose [
      choice (Monitor.get_next_error monitor) (fun e -> Error e);
      choice (try_with ~name:"Rpc.Connection.collect_errors" f) Fn.id;
    ]
  ;;

  type transport_maker = Fd.t -> max_message_size:int -> Transport.t

  let default_transport_maker fd ~max_message_size =
    Transport.of_fd fd ~max_message_size
  ;;

  let serve
        ~implementations
        ~initial_connection_state
        ~where_to_listen
        ?max_connections
        ?max_pending_connections
        ?(max_message_size=default_max_message_size)
        ?(make_transport=default_transport_maker)
        ?handshake_timeout
        ?heartbeat_config
        ?(auth = (fun _ -> true))
        ?(on_handshake_error = `Ignore)
        () =
    Tcp.Server.create_sock ?max_connections ?max_pending_connections where_to_listen
      ~on_handler_error:`Ignore
      (fun inet socket ->
         if not (auth inet) then
           Deferred.unit
         else begin
           let description =
             Info.create "TCP server listening on"
               (Tcp.Where_to_listen.address where_to_listen :> Socket.Address.t)
               Socket.Address.sexp_of_t
           in
           let connection_state = initial_connection_state inet in
           let transport = make_transport ~max_message_size (Socket.fd socket) in
           collect_errors transport ~f:(fun () ->
             Rpc_kernel.Connection.create
               ?handshake_timeout:(Option.map handshake_timeout ~f:Time_ns.Span.of_span)
               ?heartbeat_config:(Option.map heartbeat_config
                                    ~f:Heartbeat_config.v2_of_v1)
               ~implementations ~description ~connection_state transport
             >>= function
             | Ok t -> close_finished t
             | Error handshake_error ->
               begin match on_handshake_error with
               | `Call f -> f handshake_error
               | `Raise  -> raise handshake_error
               | `Ignore -> ()
               end;
               Deferred.unit)
           >>= fun res ->
           Transport.close transport
           >>| fun () ->
           Result.ok_exn res
         end)
  ;;

  module Client_implementations = struct
    type nonrec 's t =
      { connection_state : t -> 's
      ; implementations  : 's Implementations.t
      }

    let null () =
      { connection_state = (fun _ -> ())
      ; implementations  = Implementations.null ()
      }
  end

  let client ~host ~port
        ?via_local_interface
        ?implementations
        ?(max_message_size=default_max_message_size)
        ?(make_transport=default_transport_maker)
        ?(handshake_timeout=
          Time_ns.Span.to_span
            Async_rpc_kernel.Connection.default_handshake_timeout)
        ?heartbeat_config
        ?description
        () =
    let finish_handshake_by =
      Time_ns.add (Time_ns.now ()) (Time_ns.Span.of_span handshake_timeout)
    in
    Monitor.try_with (fun () ->
      Tcp.connect_sock ~timeout:handshake_timeout
        (Tcp.to_host_and_port ?via_local_interface host port))
    >>=? fun sock ->
    let description =
      match description with
      | None ->
        Info.create "Client connected via TCP" (host, port) <:sexp_of< string * int >>
      | Some desc ->
        Info.tag_arg desc "via TCP" (host, port) <:sexp_of< string * int >>
    in
    let handshake_timeout = Time_ns.diff finish_handshake_by (Time_ns.now ()) in
    let transport = make_transport (Socket.fd sock) ~max_message_size in
    begin
      match implementations with
      | None ->
        let { Client_implementations. connection_state; implementations } =
          Client_implementations.null ()
        in
        Rpc_kernel.Connection.create transport ~handshake_timeout
          ?heartbeat_config:(Option.map heartbeat_config ~f:Heartbeat_config.v2_of_v1)
          ~implementations ~description ~connection_state
      | Some { Client_implementations. connection_state; implementations } ->
        Rpc_kernel.Connection.create transport ~handshake_timeout
          ?heartbeat_config:(Option.map heartbeat_config ~f:Heartbeat_config.v2_of_v1)
          ~implementations ~description ~connection_state
    end >>= function
    | Ok _ as ok -> return ok
    | Error _ as error ->
      Transport.close transport
      >>= fun () ->
      return error

  let with_client ~host ~port
        ?via_local_interface
        ?implementations
        ?max_message_size
        ?make_transport
        ?handshake_timeout
        ?heartbeat_config
        f =
    client ?via_local_interface ~host ~port
      ?implementations
      ?max_message_size
      ?make_transport
      ?handshake_timeout
      ?heartbeat_config
      ()
    >>=? fun t ->
    try_with (fun () -> f t)
    >>= fun result ->
    close t ~reason:(Info.of_string "Rpc.Connection.with_client finished")
    >>| fun () ->
    result
end

TEST_UNIT "Open dispatches see connection closed error" =
  Thread_safe.block_on_async_exn (fun () ->
    let bin_t = Bin_prot.Type_class.bin_unit in
    let rpc =
      Rpc.create ~version:1
        ~name:"__TEST_Async_rpc.Rpc" ~bin_query:bin_t ~bin_response:bin_t
    in
    let serve () =
      let implementation =
        Rpc.implement rpc (fun () () ->
          Deferred.never ())
      in
      let implementations =
        Implementations.create_exn
          ~implementations:[ implementation ] ~on_unknown_rpc:`Raise
      in
      Connection.serve
        ~initial_connection_state:(fun _ _ -> ()) ~implementations
        ~where_to_listen:Tcp.on_port_chosen_by_os
        ()
    in
    let client ~port =
      Connection.client ~host:"localhost" ~port ()
      >>| Result.ok_exn
      >>= fun connection ->
      let res = Rpc.dispatch rpc connection () in
      don't_wait_for (
        Clock.after (sec 0.1)
        >>= fun () -> Connection.close connection);
      Clock.with_timeout (sec 0.2) res
      >>| function
      | `Timeout -> failwith "Dispatch should have gotten an error within 0.2s"
      | `Result (Ok ()) -> failwith "Dispatch should have failed"
      | `Result (Error err) ->
        assert ("(rpc Connection_closed)" = Error.to_string_hum err);
    in
    serve ()
    >>= fun server ->
    let port = Tcp.Server.listening_on server in
    client ~port
    >>= fun () -> Tcp.Server.close server)
