open Core.Std
open Async.Std

module Protocol = struct
  include Versioned_typed_tcp.Datumable_of_binable.One_version
      (struct
        open Versioned_typed_tcp.Version
        let low_version = of_int 1
        let prod_version = of_int 1
      end)
      (String) (String)
      (struct
        let of_v x = Some x
        let to_v x = Some x
      end)

  type t = datum
end

module Z = Versioned_typed_tcp.Make (struct
    module To_server_msg = Protocol
    module To_client_msg = Protocol
    module Server_name = String
    module Client_name = String
    module Mode = Versioned_typed_tcp.Dont_care_about_mode
  end)

module Server = Z.Server
module Client = Z.Client

let server_name = "test-server"
let client_name = "test-client"

let server listen_port =
  Server.create
    ~listen_port
    server_name
  >>= fun server ->
  Stream.iter_durably (Server.listen server) ~f:(function
    | Data _ -> ()
    | Control (Connect (client, _)) ->
      printf !"server: %s connected\n" client;
      Server.close server client;
    | Control (Disconnect (client, sexp)) ->
      printf !"server: %s disconnected because of %{Sexp}\n" client sexp;
    | _ -> ());
  Deferred.never ()
;;

let client server_port =
  let client =
    Client.create
      ~ip:"127.0.0.1"
      ~port:server_port
      ~expected_remote_name:server_name
      client_name
  in
  Stream.iter_durably (Client.listen client) ~f:(function
    | Data _ -> ()
    | Control (Connect (server, _)) ->
      printf !"client: connected to %s\n" server;
    | Control (Disconnect (server, sexp)) ->
      printf !"client: %s disconnected because of %{Sexp}\n" server sexp;
      upon Deferred.unit (fun () -> Client.send_ignore_errors client "msg");
    | _  -> ());
  Client.send_ignore_errors client "msg";
  Deferred.never ()
;;

let cmd =
  Command.async ~summary:"test connection rate"
    (Command.Spec.(empty +> anon ("server-port" %: int)))
    (fun port () ->
       Deferred.all_unit
         [ server port
         ; client port
         ])

let () = Command.run cmd
