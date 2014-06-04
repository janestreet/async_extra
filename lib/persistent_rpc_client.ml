open Core.Std
open Import

module Event = struct

  type t = [
    | `Attempting_to_connect
    | `Obtained_address of Host_and_port.t
    | `Failed_to_connect of Error.t
    | `Connected
    | `Disconnected
  ] with sexp

  type event = t

  module Handler = struct
    type t = {
      server_name : string;
      on_event : event -> unit;
      log : Log.t option;
    }
  end

  let log_level = function
    | `Attempting_to_connect | `Connected | `Disconnected | `Obtained_address _ -> `Info
    | `Failed_to_connect _ -> `Error

  let handle t {Handler.server_name; log; on_event} =
    on_event t;
    Option.iter log ~f:(fun log ->
      Log.sexp log t sexp_of_t ~level:(log_level t)
        ~tags:[("persistent-connection-to", server_name)])

end

type t = {
  get_address : unit -> Host_and_port.t Or_error.t Deferred.t;
  connect : host:string -> port:int -> (Rpc.Connection.t, exn) Result.t Deferred.t;
  mutable conn : Rpc.Connection.t Ivar.t;
  event_handler : Event.Handler.t;
} with fields

let handle_event t event = Event.handle event t.event_handler

(* How long to wait between connection attempts.  This value is randomized to avoid all
   clients hitting the server at the same time. *)
let retry_delay () = Time.Span.randomize ~percent:0.3 (sec 10.)

(* This function focuses in on the the error itself, discarding information about which
   monitor caught the error, if any.

   If we don't do this, we sometimes end up with noisy logs which report the same error
   again and again, differing only as to what monitor caught them. *)
let same_error e1 e2 =
  let to_sexp e = Exn.sexp_of_t (Monitor.extract_exn e) in
  Sexp.equal (to_sexp e1) (to_sexp e2)

let try_connecting_until_successful t =
  (* We take care not to spam logs with the same message over and over by comparing
     each log message the the previous one of the same type. *)
  let previous_address = ref None in
  let previous_error   = ref None in
  let connect () =
    t.get_address ()
    >>= function
    | Error e -> return (Error (Error.to_exn e))
    | Ok addr ->
      let same_as_previous_address =
        match !previous_address with
        | None -> false
        | Some previous_address -> Host_and_port.equal addr previous_address
      in
      previous_address := Some addr;
      if not same_as_previous_address then handle_event t (`Obtained_address addr);
      t.connect
        ~host:(Host_and_port.host addr)
        ~port:(Host_and_port.port addr)
  in
  let rec loop () =
    connect ()
    >>= function
    | Ok conn -> return conn
    | Error err ->
      let same_as_previous_error =
        match !previous_error with
        | None -> false
        | Some previous_err -> same_error err previous_err
      in
      previous_error := Some err;
      if not same_as_previous_error then
        handle_event t (`Failed_to_connect (Error.of_exn err));
      after (retry_delay ())
      >>= fun () ->
      loop ()
  in
  loop ()

let create ~server_name ?log ?(on_event = ignore) ?implementations
      ?max_message_size ?handshake_timeout get_address =
  let event_handler = {Event.Handler.server_name; log; on_event} in
  (* package up the call to [Rpc.Connection.client] so that the polymorphism over
     [implementations] is hidden away *)
  let connect ~host ~port =
    Rpc.Connection.client ~host ~port
      ?implementations ?max_message_size ?handshake_timeout ()
  in
  let t = {event_handler; get_address; connect; conn = Ivar.create ()} in
  Deferred.forever () (fun () ->
    handle_event t `Attempting_to_connect;
    let ready_to_retry_connecting = after (retry_delay ()) in
    try_connecting_until_successful t
    >>= fun conn ->
    Ivar.fill t.conn conn;
    handle_event t `Connected;
    Rpc.Connection.close_finished conn
    >>= fun () ->
    t.conn <- Ivar.create ();
    handle_event t `Disconnected;
    (* waits until [retry_delay ()] time has passed since the time just before we last
       tried to connect rather than the time we noticed being disconnected, so that if a
       long-lived connection dies, we will attempt to reconnect immediately. *)
    ready_to_retry_connecting
  );
  t

let connected t = Ivar.read t.conn

let current_connection t = Ivar.read t.conn |> Deferred.peek
