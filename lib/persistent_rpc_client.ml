open Core.Std
open Import

module Event = struct

  type t =
    | Attempting_to_connect
    | Obtained_address of Host_and_port.t
    | Failed_to_connect of Error.t
    | Connected
    | Disconnected
  with sexp

  type event = t

  module Handler = struct
    type t = {
      server_name : string;
      on_event : event -> unit;
      log : Log.t option;
    }
  end

  let log_level = function
    | Attempting_to_connect | Connected | Disconnected | Obtained_address _ -> `Info
    | Failed_to_connect _ -> `Error

  let handle t {Handler.server_name; log; on_event} =
    on_event t;
    Option.iter log ~f:(fun log ->
      Log.sexp log t sexp_of_t ~level:(log_level t)
        ~tags:[("persistent-connection-to", server_name)])

end

type t = {
  get_address : unit -> Host_and_port.t Or_error.t Deferred.t;
  connect : host:string -> port:int -> (Rpc.Connection.t, exn) Result.t Deferred.t;
  mutable conn : Rpc.Connection.t option Ivar.t; (* None iff [close] has been called *)
  event_handler : Event.Handler.t;
  close_started : unit Ivar.t;
  close_finished : unit Ivar.t;
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
      if not same_as_previous_address then handle_event t (Obtained_address addr);
      t.connect
        ~host:(Host_and_port.host addr)
        ~port:(Host_and_port.port addr)
  in
  let rec loop () =
    if Ivar.is_full t.close_started then
      return None
    else begin
      connect ()
      >>= function
      | Ok conn -> return (Some conn)
      | Error err ->
        let same_as_previous_error =
          match !previous_error with
          | None -> false
          | Some previous_err -> same_error err previous_err
        in
        previous_error := Some err;
        if not same_as_previous_error then
          handle_event t (Failed_to_connect (Error.of_exn err));
        after (retry_delay ())
        >>= fun () ->
        loop ()
    end
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
  let t =
    { event_handler; get_address; connect; conn = Ivar.create ();
      close_started = Ivar.create (); close_finished = Ivar.create () }
  in
  don't_wait_for @@ Deferred.repeat_until_finished () (fun () ->
    handle_event t Attempting_to_connect;
    let ready_to_retry_connecting = after (retry_delay ()) in
    try_connecting_until_successful t
    >>= fun maybe_conn ->
    Ivar.fill t.conn maybe_conn;
    match maybe_conn with
    | None -> return (`Finished ())
    | Some conn ->
      handle_event t Connected;
      Rpc.Connection.close_finished conn
      >>= fun () ->
      t.conn <- Ivar.create ();
      handle_event t Disconnected;
      (* waits until [retry_delay ()] time has passed since the time just before we last
         tried to connect rather than the time we noticed being disconnected, so that if a
         long-lived connection dies, we will attempt to reconnect immediately. *)
      Deferred.any [
        (ready_to_retry_connecting >>| fun () -> `Repeat ());
        (Ivar.read t.close_started >>| fun () -> `Finished ())
      ]
  );
  t

let connected t =
  (* Take care not to return a connection that is known to be closed at the time
     [connected] was called.  This could happen in client code that behaves like
     {[
       Persistent_rpc_client.connected t
       >>= fun c1 ->
       ...
       Rpc.Connection.close_finished c1
       (* at this point we are in a race with the same call inside persistent_client.ml *)
       >>= fun () ->
       Persistent_rpc_client.connected t
       (* depending on how the race turns out, we don't want to get a closed connection
          here *)
       >>= fun c2 ->
       ...
     ]}
     This doesn't remove the race condition, but it makes it less likely to happen.
  *)
  let rec loop () =
    let d = Ivar.read t.conn in
    match Deferred.peek d |> Option.join with
    | None ->
      begin
        d >>= function
        | None -> Deferred.never ()
        | Some conn -> return conn
      end
    | Some conn ->
      if Rpc.Connection.is_closed conn then
        (* give the reconnection loop a chance to overwrite the ivar *)
        Rpc.Connection.close_finished conn >>= loop
      else
        return conn
  in
  loop ()

let current_connection t = Ivar.read t.conn |> Deferred.peek |> Option.join

let close_finished t = Ivar.read t.close_finished

let close t =
  if Ivar.is_full t.close_started then
    (* Another call to close is already in progress.  Wait for it to finish. *)
    close_finished t
  else begin
    Ivar.fill t.close_started ();
    Ivar.read t.conn
    >>= fun conn_opt ->
    begin
      match conn_opt with
      | None -> Deferred.unit
      | Some conn -> Rpc.Connection.close conn
    end
    >>| fun () ->
    Ivar.fill t.close_finished ()
  end
