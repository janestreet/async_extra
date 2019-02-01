module Persistent_connection_intf_in_this_directory = Persistent_connection_intf
open Core
open Import
module Persistent_connection_intf = Persistent_connection_intf_in_this_directory
include Persistent_connection_intf

module Make (Conn : T) = struct
  include Async_kernel_persistent_connection.Make (Conn)

  let create
        ~server_name
        ?log
        ?(on_event = fun _ -> Deferred.unit)
        ?retry_delay
        ~connect
        get_address
    =
    let retry_delay =
      Option.map retry_delay ~f:(fun f () ->
        f () |> Time_ns.Span.of_span_float_round_nearest)
    in
    let on_event event =
      Option.iter log ~f:(fun log ->
        if Log.would_log log (Some (Event.log_level event))
        then
          Log.sexp
            log
            ~tags:[ "persistent-connection-to", server_name ]
            ~level:(Event.log_level event)
            (Event.sexp_of_t event));
      on_event event
    in
    create ~server_name ~on_event ?retry_delay ~connect get_address
  ;;
end

module Versioned_rpc = Make (struct
    module Address = Host_and_port

    type t = Versioned_rpc.Connection_with_menu.t

    let rpc_connection = Versioned_rpc.Connection_with_menu.connection
    let close t = Rpc.Connection.close (rpc_connection t)
    let is_closed t = Rpc.Connection.is_closed (rpc_connection t)
    let close_finished t = Rpc.Connection.close_finished (rpc_connection t)
  end)

module Rpc = struct
  include Make (struct
      module Address = Host_and_port

      type t = Rpc.Connection.t

      let close t = Rpc.Connection.close t
      let is_closed t = Rpc.Connection.is_closed t
      let close_finished t = Rpc.Connection.close_finished t
    end)

  (* convenience wrapper *)
  let create'
        ~server_name
        ?log
        ?on_event
        ?retry_delay
        ?bind_to_address
        ?implementations
        ?max_message_size
        ?make_transport
        ?handshake_timeout
        ?heartbeat_config
        get_address
    =
    let connect host_and_port =
      Rpc.Connection.client
        (Tcp.Where_to_connect.of_host_and_port ?bind_to_address host_and_port)
        ?implementations
        ?max_message_size
        ?make_transport
        ?handshake_timeout
        ?heartbeat_config
        ~description:(Info.of_string ("persistent connection to " ^ server_name))
      >>| Or_error.of_exn_result
    in
    create ~server_name ?log ?on_event ?retry_delay ~connect get_address
  ;;
end
