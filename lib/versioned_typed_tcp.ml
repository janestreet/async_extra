
open Core.Std
open Import

exception Bigsubstring_allocator_got_invalid_requested_size of int with sexp

let bigsubstring_allocator ?(initial_size = 512) () =
  let buf = ref (Bigstring.create initial_size) in
  let rec alloc requested_size =
    if requested_size < 1 then
      raise (Bigsubstring_allocator_got_invalid_requested_size requested_size);
    if requested_size > Bigstring.length !buf then
      buf := (Bigstring.create
                (Int.max requested_size (2 * Bigstring.length !buf)));
    Bigsubstring.create !buf ~pos:0 ~len:requested_size
  in
  alloc
;;

module type Name = sig
  type t
  include Hashable with type t := t
  include Binable with type t := t
  include Stringable with type t := t
  include Comparable with type t := t
end

(* estokes tried changing the type of [marshal_fun] so that it returned a variant
   indicating conversion failure, and decided it was too messy.  So, the plan is to use
   exceptions for conversion failure.  And that this is OK, because that case is usually a
   bug. *)
type 'a marshal_fun = 'a -> Bigsubstring.t option
type 'a unmarshal_fun = Bigsubstring.t -> 'a option

let protocol_version : [ `Prod | `Test ] ref = ref `Test

module Version : sig
  type t with bin_io, sexp

  val min : t -> t -> t
  val of_int : int -> t
  val to_int : t -> int
  val add : t -> int -> t

  include Hashable with type t := t
end = struct
  include Int
  let add = (+)
end

module type Versions = sig
  val low_version : Version.t
  val prod_version : Version.t
  val test_version : Version.t
end

(** This module describes the type of a given direction of message
    flow. For example it might describe the type of messages from the
    client to the server.  *)
module type Datumable = sig
  type datum
  include Versions

  (** [lookup_marshal_fun v] This function takes a version [v], and returns a
      function that will downgrade (if necessary) the current version to [v] and
      then write it to a bigsubstring. It is perfectly fine if one message
      becomes zero or more messages as a result of downgrading, this is why the
      marshal fun returns a list. The contents of these buffers will be copied
      immediatly, so it is safe to reuse the same bigstring for multiple
      marshals.
  *)
  val lookup_marshal_fun : Version.t -> (datum marshal_fun, exn) Result.t

  (** [lookup_unmarshal_fun v] This function takes a version [v], and returns a
      function that unmarshals a message and upgrades it, returning zero or more
      messages as a result of the upgrade. The bigsubstring is only guaranteed
      to contain valid data until the unmarshal function returns, after which it
      may be overwritten immediatly. *)
  val lookup_unmarshal_fun : Version.t -> (datum unmarshal_fun, exn) Result.t
end

module type Datum = sig
  type t
  include Datumable with type datum = t
end

(** This module may be used to implement modes for clients/servers. A
    common scheme is to have two modes, Test, and Production, and to
    want to maintain the invariant that clients in mode Test may not
    talk to servers in mode Production, and that clients in mode
    Production may not talk to servers in mode Test. Versioned
    connection will check that the mode of the client is the same as
    the mode of the server.

    If you don't care about modes, just use Dont_care_about_mode. *)
module type Mode = sig
  type t

  val current : unit -> t
  val (=) : t -> t -> bool

  include Binable with type t := t
  include Sexpable with type t := t
end

module Dont_care_about_mode = struct
  type t = Dont_care_about_mode with bin_io, sexp


  let current () = Dont_care_about_mode
  let (=) (a : t) b = a = b
end

module Read_result = struct
  type ('name, 'data) t = {
    from : 'name;
    ip : string;
    time_received : Time.t;
    time_sent : Time.t;
    data : 'data;
  } with bin_io, sexp
end

module Server_msg = struct
  module Control = struct
    type 'name t =
    | Unauthorized of string
    | Duplicate of 'name
    | Wrong_mode of 'name
    | Too_many_clients of string
    | Almost_full of int (* number of free connections *)
    | Connect of 'name
    | Disconnect of 'name * Sexp.t
    | Parse_error of 'name * string
    | Protocol_error of string
    with sexp, bin_io
  end

  type ('name, 'data) t =
  | Control of 'name Control.t
  | Data of ('name, 'data) Read_result.t
end

module Client_msg = struct
  module Control = struct
    type 'name t =
    | Connecting
    | Connect of 'name
    | Disconnect of 'name * Sexp.t
    | Parse_error of 'name * string
    | Protocol_error of string
    with sexp, bin_io
  end

  type ('name, 'data) t =
  | Control of 'name Control.t
  | Data of ('name, 'data) Read_result.t
  with bin_io, sexp
end

module type Arg = sig
  module Send : Datum
  module Recv : Datum
  module Remote_name : Name
  module My_name : Name
  module Mode : Mode
end

module type S = sig
  include Arg

  type logfun =
  [ `Recv of Recv.t | `Send of Send.t ]
    -> Remote_name.t
    -> time_sent_received:Time.t
    -> unit

  module Server : sig
    type t


    (** create a new server, and start listening *)
    val create :
      ?logfun:logfun
      -> ?now:(unit -> Time.t) (** defualt: Scheduler.cycle_start *)
      -> ?enforce_unique_remote_name:bool (** remote names must be unique, default true *)
      -> ?is_client_ip_authorized:(string -> bool)
      (** [warn_when_free_connections_lte_pct].  If the number of free connections falls
          below this percentage of max connections an Almost_full event will be generated.
          The default is 5%.  It is required that 0.0 <=
          warn_when_free_connections_lte_pct <= 1.0 *)
      -> ?warn_when_free_connections_lte_pct:float
      -> ?max_clients:int (** max connected clients. default 500 *)
      -> listen_port:int
      -> My_name.t
      -> t Deferred.t

    (** get the port that the server is listening on *)
    val port : t -> int

    (** [close t client] close connection to [client] if it
        exists. This does not prevent the same client from connecting
        again later. *)
    val close : t -> Remote_name.t -> unit

    (** [listen t] listen to the stream of messages and errors coming from clients *)
    val listen : t -> (Remote_name.t, Recv.t) Server_msg.t Stream.t

    (** [listen_ignore_errors t] like listen, but omit error conditions and
        metadata. When listen_ignore_errors is called it installs a filter on
        the stream that never goes away (unless t is destroyed, or you
        provide a [stop]). *)
    val listen_ignore_errors : ?stop:unit Deferred.t -> t -> Recv.t Stream.t

    (** [send t client msg] send [msg] to [client]. @return a
        deferred that will become determined when the message has been
        sent.  In the case of an error, the message will be dropped,
        and the deferred will be filled with [`Dropped] (meaning the
        message was never handed to the OS), otherwise it will be
        filled with with [`Sent tm] where tm is the time (according to
        Time.now) that the message was handed to the operating
        system.  It is possible that the deferred will never become
        determined, for example in the case that the other side hangs,
        but does not drop the connection. *)
    val send :
      t -> Remote_name.t -> Send.t -> [ `Sent of Time.t | `Dropped ] Deferred.t

    (** [send_ignore_errors t client msg] Just like send, but does not report
        results. Your message will probably be sent successfully
        sometime after you call this function. If you receive a
        [Disconnect] error on the listen channel in close time
        proximity to making this call then your message was likely
        dropped. *)
    val send_ignore_errors : t -> Remote_name.t -> Send.t -> unit

    (** [send_to_all t msg] send the same message to all connected clients. *)
    val send_to_all : t
      -> Send.t
      -> [ `Sent (** sent successfuly to all clients *)
           | `Dropped (** not sent successfully to any client *)
           | `Partial_success (** sent to some clients *)] Deferred.t

    (** [send_to_all_ignore_errors t msg] Just like [send_to_all] but with no error
        reporting. *)
    val send_to_all_ignore_errors : t -> Send.t -> unit

    val flushed :
      t
      -> cutoff:unit Deferred.t
      -> ( [ `Flushed of Remote_name.t list ]
           * [ `Not_flushed of Remote_name.t list ] ) Deferred.t
  end

  module Client : sig
    type t

    (** create a new (initially disconnected) client *)
    val create :
      ?logfun:logfun
      -> ?now:(unit -> Time.t) (** defualt: Scheduler.cycle_start *)
      -> ?check_remote_name:bool (** remote name must match expected remote name. default true *)
      -> ip:string
      -> port:int
      -> expected_remote_name:Remote_name.t
      -> My_name.t
      -> t Deferred.t

    (** [connect t] If the connection is not currently established, initiate one.
        @return a deferred that becomes determined when the connection is established. *)
    val connect : t -> unit Deferred.t

    (** If a connection is currently established, close it. *)
    val close_connection : t -> unit

    (** [listen t] @return a stream of messages from the server and errors *)
    val listen : t -> (Remote_name.t, Recv.t) Client_msg.t Stream.t

    (** [listen_ignore_errors t] like [listen], but with no errors or meta data.  When
        listen_ignore_errors is called it installs a filter on the stream that never
        goes away (unless t is destroyed or you provide a stop), so you should
        not call it many times throwing away the result.  If you need to do this
        use listen. *)
    val listen_ignore_errors : ?stop:unit Deferred.t -> t -> Recv.t Stream.t

    (** [send t msg] send a message to the server. If the connection is
        not currently established, initiate one.
        @return a deferred that is filled in with either the time the
        message was handed to the OS, or [`Dropped]. [`Dropped] means that
        there was an error, and the message will not be sent. *)
    val send : t -> Send.t -> [`Sent of Time.t | `Dropped] Deferred.t

    (** [send_ignore_errors t] exactly like [send] but with no error reporting. *)
    val send_ignore_errors : t -> Send.t -> unit

    (** [state t] @return the state of the connection *)
    val state : t -> [`Disconnected | `Connected | `Connecting]

    (** [last_connect_error t] returns the error (if any) that happened on the
        last connection attempt. *)
    val last_connect_error : t -> exn option
  end
end

module Make (Z : Arg) :
  S with
    module Send = Z.Send and
    module Recv = Z.Recv and
    module My_name = Z.My_name and
    module Remote_name = Z.Remote_name = struct
  include Z

  module Constants = struct
    let negotiate_timeout = sec 10.
    let wait_after_exn = sec 0.5

    let wait_after_connect_failure = sec 4.
    let wait_after_too_many_clients = sec 0.5
    let connect_timeout = sec 10.
  end

  open Constants

  type logfun =
  [ `Recv of Recv.t | `Send of Send.t ]
    -> Remote_name.t
    -> time_sent_received:Time.t
    -> unit

  (* mstanojevic: note that Hello.t contains Mode, which means that we
     can't ever change the Mode type! *)
  module Hello = struct
    type t = {
      name : string;
      mode : Mode.t;
      send_version : Version.t;
      recv_version : Version.t;
      credentials : string; (* For future use *)
    }
    with sexp, bin_io

    let create ~name ~send_version ~recv_version ~credentials =
      { name;
        mode = Mode.current ();
        send_version;
        recv_version;
        credentials;
      }
  end

  (* After negotiation messages on the wire are composed of a header,
     and then a message body *)
  module Message_header = struct
    type t = {
      time_stamp: Time.t;
      body_length: int;
    } with sexp, bin_io
  end

  module Connection = struct
    type t = {
      writer : Writer.t;
      reader : Reader.t;
      marshal_fun : Send.t marshal_fun;
      unmarshal_fun : Recv.t unmarshal_fun;
      send_version : Version.t;
      name : Remote_name.t;
      kill: unit -> unit;
    }

    let kill t = t.kill ()
  end

  let try_with = Monitor.try_with

  let ignore_errors f = whenever (try_with f >>| ignore)

  let try_with_timeout span f =
    choose
      [ choice (Clock.after span) (fun () -> `Timeout);
        choice (try_with f) (function
        | Ok x -> `Ok x
        | Error x -> `Error x)
      ]
  ;;

  module Write_bin_prot_error = struct
    type t = {
      name : string;
      arg : Sexp.t;
      exn : exn;
      backtrace : string;
    } with sexp_of
    exception E of t with sexp
  end

  let wrap_write_bin_prot ~sexp ~tc ~writer ~name m =
    try Writer.write_bin_prot writer tc m
    with exn ->
      let module W = Write_bin_prot_error in
      raise (W.E { W.
                   name;
                   arg = sexp m;
                   exn;
                   backtrace = Exn.backtrace ();
                 })
  ;;

  let send_raw ~writer ~hdr ~msg =
    let module H = Message_header in
    wrap_write_bin_prot
      ~sexp:H.sexp_of_t ~tc:H.bin_t.Bin_prot.Type_class.writer ~writer
      ~name:"send" hdr;
    Writer.write_bigsubstring writer msg
  ;;

  let send_no_flush =
    let module C = Connection in
    let module H = Message_header in
    let maybe_log ~logfun ~name ~now d =
      match logfun with
      | None -> ()
      | Some f -> f (`Send d) name ~time_sent_received:now
    in
    fun ~logfun ~name ~now con d ->
      match con.C.marshal_fun d with
      | None -> `Not_sent
      | Some msg ->
        let now = now () in
        let hdr = {H.time_stamp = now; body_length = Bigsubstring.length msg} in
        send_raw ~writer:con.C.writer ~hdr ~msg;
        maybe_log ~logfun ~name ~now d;
        `Sent
  ;;

  let send ~logfun ~name ~now con d =
    match send_no_flush ~logfun:None ~name ~now con d with
    | `Sent ->
      Writer.flushed_time con.Connection.writer >>| fun tm ->
      begin match logfun with
      | None -> ()
      | Some f -> f (`Send d) name ~time_sent_received:(now ())
      end;
      `Sent tm
    | `Not_sent -> return `Dropped
  ;;

  let negotiate ~reader ~writer ~my_name ~auth_error =
    let (recv_version, send_version) =
      match !protocol_version with
      | `Prod -> (Recv.prod_version, Send.prod_version)
      | `Test -> (Recv.test_version, Send.test_version)
    in
    let h =
      Hello.create
        ~name:my_name
        ~send_version
        ~recv_version
        ~credentials:""
    in
    wrap_write_bin_prot ~sexp:Hello.sexp_of_t ~tc:Hello.bin_writer_t
      ~writer ~name:"negotiate" h;
    Reader.read_bin_prot reader Hello.bin_reader_t >>| (function
    | `Eof -> `Eof
    | `Ok h ->
      match auth_error h with
      | Some e -> `Auth_error e
      | None ->
        let recv_version = Version.min recv_version h.Hello.send_version in
        let send_version = Version.min send_version h.Hello.recv_version in
        match Recv.lookup_unmarshal_fun recv_version with
        | Error _ -> `Version_error
        | Ok unmarshal_fun ->
          match Send.lookup_marshal_fun send_version with
          | Error _ -> `Version_error
          | Ok marshal_fun -> `Ok (h, send_version, marshal_fun, unmarshal_fun))
  ;;

  exception Eof with sexp

  let handle_incoming
      ~logfun ~remote_name ~ip ~con
      (* functions to extend the tail (with various messages) *)
      ~extend_disconnect ~extend_parse_error ~extend_data =
    let module C = Connection in
    let module H = Message_header in
    Stream.iter (Monitor.errors (Writer.monitor con.C.writer)) ~f:(fun e ->
      con.C.kill ();
      extend_disconnect remote_name e);
    let upon_ok a f =
      upon a (function
      | Error e ->
        con.C.kill ();
        extend_disconnect remote_name e
      | Ok `Eof ->
        con.C.kill ();
        extend_disconnect remote_name Eof
      | Ok (`Ok msg) -> f msg)
    in
    let alloc = bigsubstring_allocator () in
    let read_header () = Reader.read_bin_prot con.C.reader H.bin_reader_t in
    let get_substring hdr = alloc hdr.H.body_length in
    let read_ss ss =
      Reader.really_read_bigsubstring con.C.reader ss >>| function
      | `Ok -> `Ok ()
      | `Eof _ -> `Eof
    in
    let extend ~time_sent ~time_received data =
      begin match logfun with
      | None -> ()
      | Some f ->
        f (`Recv data) remote_name
          ~time_sent_received:time_received
      end;
      extend_data
        { Read_result.
          from = remote_name;
          data;
          ip;
          time_received;
          time_sent;
        }
    in
    let rec loop () =
      upon_ok (Monitor.try_with read_header) (fun hdr ->
        let msg_ss = get_substring hdr in
        upon_ok (Monitor.try_with (fun () -> read_ss msg_ss)) (fun () ->
          let res = try Ok (con.C.unmarshal_fun msg_ss) with e -> Error e in
          begin match res with
          | Ok (Some msg) ->
            let time_received = Reader.last_read_time con.C.reader in
            let time_sent = hdr.H.time_stamp in
            extend ~time_received ~time_sent msg
          | Ok None -> ()
          | Error exn -> extend_parse_error remote_name (Exn.to_string exn)
          end;
          loop ()))
    in
    loop ()
  ;;

  module Server = struct

    module Connections : sig
      type t
      val create : unit -> t
      val mem : t -> Remote_name.t -> bool
      val find : t -> Remote_name.t -> Connection.t option
      val add : t -> name:Remote_name.t -> conn:Connection.t -> unit
      val remove : t -> Remote_name.t -> unit

      val fold :
        t
        -> init:'a
        -> f:(name:Remote_name.t -> conn:Connection.t -> 'a-> 'a)
        -> 'a

      val send_to_all : t
      -> logfun:logfun option
      -> now:(unit -> Time.t)
      -> Send.t
      -> [ `Sent (** sent successfuly to all clients *)
         | `Dropped (** not sent successfully to any client *)
         | `Partial_success (** sent to some clients *)] Deferred.t

      val send_to_all_ignore_errors : t
        -> logfun:logfun option
        -> now:(unit -> Time.t)
        -> Send.t
        -> unit

    end = struct

      module C = Connection

      type t = {
        by_name : (C.t Bag.t * C.t Bag.Elt.t) Remote_name.Table.t;
        by_send_version : (C.t Bag.t * Send.t marshal_fun) Version.Table.t;
      }

      let create () =
        { by_name = Remote_name.Table.create ();
          by_send_version =
            Version.Table.create
              ~size:(Version.to_int Send.test_version) ();
        }
      ;;

      let fold t ~init ~f =
        Hashtbl.fold t.by_name ~init ~f:(fun ~key ~data:(_, bag_elt) acc ->
          f ~name:key ~conn:(Bag.Elt.value bag_elt) acc)
      ;;

      let mem t name = Hashtbl.mem t.by_name name

      let add t ~name ~conn =
        let bag, _marshal_fun =
          Hashtbl.find_or_add t.by_send_version conn.C.send_version
            ~default:(fun () -> Bag.create (), conn.C.marshal_fun)
        in
        let bag_elt = Bag.add bag conn in
        Hashtbl.replace t.by_name ~key:name ~data:(bag, bag_elt);
      ;;

      let remove t name =
        match Hashtbl.find t.by_name name with
        | None -> ()
        | Some (bag, bag_elt) ->
          Bag.remove bag bag_elt;
          Hashtbl.remove t.by_name name;
      ;;

      let find t name =
        match Hashtbl.find t.by_name name with
        | None -> None
        | Some (_, bag_elt) -> Some (Bag.Elt.value bag_elt)
      ;;

      let maybe_log ~logfun ~name ~now d =
        match logfun with
        | None -> ()
        | Some f -> f (`Send d) name ~time_sent_received:now
      ;;

      let schedule_bigstring_threshold = 64 * 1024 (* half the size of writer's buffer *)

      let send_to_all' t d ~logfun ~now =
        let module C = Connection in
        let module H = Message_header in
        let now = now () in
        let res =
          Hashtbl.fold t.by_send_version ~init:`Init
            ~f:(fun ~key:_ ~data:(bag, marshal_fun) acc ->
              if not (Bag.is_empty bag) then begin
                let res =
                  match marshal_fun d with
                  | None -> `Dropped
                  | Some msg ->
                    let body_length = Bigsubstring.length msg in
                    let hdr =
                      { H.time_stamp = now; body_length; }
                    in
                    let send =
                      if body_length > schedule_bigstring_threshold then begin
                        let to_schedule = Bigstring.create body_length in
                        Bigsubstring.blit_to_bigstring msg ~dst:to_schedule ~dst_pos:0;
                        fun conn ->
                          wrap_write_bin_prot
                            ~sexp:H.sexp_of_t ~tc:H.bin_t.Bin_prot.Type_class.writer
                            ~writer:conn.C.writer
                            ~name:"send" hdr;
                          Writer.schedule_bigstring conn.C.writer to_schedule
                      end
                      else begin
                        fun conn ->
                          send_raw ~writer:conn.C.writer ~hdr ~msg;
                      end
                    in
                    Bag.iter bag ~f:(fun conn ->
                      send conn;
                      maybe_log ~logfun ~now ~name:conn.C.name d);
                    `Sent
                in
                match acc, res with
                | `Partial_success, _
                | `Sent           , `Dropped
                | `Dropped        , `Sent
                  -> `Partial_success
                | `Sent           , `Sent
                  -> `Sent
                | `Dropped        , `Dropped
                  -> `Dropped
                | `Init           , _
                  -> res
              end
              else
                acc)
        in
        match res with
        | `Init -> `Sent
        | `Partial_success | `Dropped | `Sent as x -> x
      ;;

      let send_to_all t ~logfun ~now d =
        let res = send_to_all' t d ~logfun ~now in
        Deferred.all_unit (fold t ~init:[] ~f:(fun ~name:_ ~conn acc ->
          Writer.flushed conn.C.writer :: acc))
        >>| fun () -> res
      ;;

      let send_to_all_ignore_errors t ~logfun ~now d =
        ignore (send_to_all' t d ~logfun ~now : [`Partial_success | `Dropped | `Sent])
      ;;

    end

    type t = {
      tail : (Remote_name.t, Recv.t) Server_msg.t Tail.t;
      logfun : logfun option;
      connections : Connections.t;
      mutable am_listening : bool;
      socket : ([ `Bound ], Socket.inet) Socket.t;
      warn_free_connections_pct: float;
      mutable free_connections: int;
      mutable when_free: unit Ivar.t option;
      max_clients: int;
      is_client_ip_authorized : string -> bool;
      my_name : My_name.t;
      enforce_unique_remote_name : bool;
      now : unit -> Time.t;
      mutable num_accepts : Int63.t;
    }

    let invariant t =
      assert (t.free_connections >= 0);
      assert (t.free_connections <= t.max_clients);
      assert ((t.free_connections = 0) = is_some t.when_free);
    ;;

    let flushed t ~cutoff =
      let flushes =
        Connections.fold t.connections ~init:[]
          ~f:(fun ~name:client ~conn acc ->
            (choose [ choice (Writer.flushed conn.Connection.writer)
                        (fun _ -> `Flushed client);
                      choice cutoff (fun () -> `Not_flushed client);
                    ]) :: acc)
      in
      Deferred.all flushes >>| fun results ->
      let (flushed, not_flushed) =
        List.partition_map results
          ~f:(function `Flushed c -> `Fst c | `Not_flushed c -> `Snd c)
      in
      `Flushed flushed, `Not_flushed not_flushed
    ;;

    let send t name d =
      match Connections.find t.connections name with
      | None -> return `Dropped
      | Some c -> send ~logfun:t.logfun ~name ~now:t.now c d
    ;;

    let send_ignore_errors t name d =
      match Connections.find t.connections name with
      | None -> ()
      | Some c -> ignore (send_no_flush ~logfun:t.logfun ~name ~now:t.now c d)
    ;;

    let send_to_all t d =
      Connections.send_to_all t.connections d ~now:t.now ~logfun:t.logfun
    ;;

    let send_to_all_ignore_errors t d =
      Connections.send_to_all_ignore_errors t.connections d ~now:t.now ~logfun:t.logfun
    ;;

    let client_is_authorized t ip =
      t.is_client_ip_authorized (Unix.Inet_addr.to_string ip)
    ;;

    let close t name =
      Option.iter (Connections.find t.connections name) ~f:Connection.kill
    ;;

    let handle_client t addr port fd =
      let module S = Server_msg in
      let module C = Server_msg.Control in
      let control e = Tail.extend t.tail (S.Control e) in
      let close () = whenever (Deferred.ignore (Unix.close fd)) in
      if not (client_is_authorized t addr) then begin
        control (C.Unauthorized (Unix.Inet_addr.to_string addr));
        close ();
      end else begin
        let r = Reader.create fd in
        let w = Writer.create ~syscall:`Per_cycle fd in
        (* we need to force close, otherwise there is a fd leak *)
        let close =
          lazy (ignore_errors (fun () ->
            Writer.close w ~force_close:(Clock.after (sec 5.))))
        in
        let kill = ref (fun () -> Lazy.force close) in
        let die_error e =
          Tail.extend t.tail (S.Control e);
          !kill ()
        in
        Stream.iter (Monitor.errors (Writer.monitor w))
          (* We don't report the error to the tail here because this is before
             protocol negotiation.  Once protocol negotiation happens,
             [handle_incoming] will report any errors to the tail.
          *)
          ~f:(fun _ -> !kill ());
        let res =
          try_with_timeout negotiate_timeout (fun () ->
            negotiate
              ~my_name:(My_name.to_string t.my_name)
              ~reader:r ~writer:w
              ~auth_error:(fun h ->
                if not (Mode.(=) h.Hello.mode (Mode.current ())) then
                  Some (C.Wrong_mode (Remote_name.of_string h.Hello.name))
                else
                  None))
        in
        upon res (function
        | `Error _
        | `Timeout
        | `Ok `Eof
        | `Ok `Version_error -> Lazy.force close
        | `Ok (`Auth_error e) -> die_error e
        | `Ok (`Ok (h, send_version, marshal_fun, unmarshal_fun)) ->
          let name =
            if t.enforce_unique_remote_name then
              h.Hello.name
            else
              sprintf "%s:%s:%d:%s"
                h.Hello.name
                (Unix.Inet_addr.to_string addr)
                port
                (Int63.to_string t.num_accepts)
          in
          match Result.try_with (fun () -> Remote_name.of_string name) with
          | Error exn ->
            die_error
              (C.Protocol_error
                 (sprintf "error constructing name: %s, error: %s"
                    name (Exn.to_string exn)))
          | Ok remote_name ->
            let module T = Remote_name.Table in
            if Connections.mem t.connections remote_name then
              die_error (C.Duplicate remote_name)
            else begin
              let close =
                lazy
                  (Lazy.force close;
                   Connections.remove t.connections remote_name)
              in
              kill := (fun () -> Lazy.force close);
              let conn =
                { Connection.
                  writer = w;
                  reader = r;
                  unmarshal_fun;
                  marshal_fun;
                  send_version;
                  name = remote_name;
                  kill = !kill }
              in
              Tail.extend t.tail (S.Control (C.Connect remote_name));
              Connections.add t.connections ~name:remote_name ~conn;
              handle_incoming
                ~logfun:t.logfun ~remote_name
                ~ip:(Unix.Inet_addr.to_string addr) ~con:conn
                ~extend_disconnect:(fun n e ->
                  Tail.extend t.tail (S.Control (C.Disconnect (n, Exn.sexp_of_t e))))
                ~extend_parse_error:(fun n e ->
                  Tail.extend t.tail (S.Control (C.Parse_error (n, e))))
                ~extend_data:(fun x -> Tail.extend t.tail (S.Data x))
            end)
      end

    let listen t =
      if not t.am_listening then begin
        let module S = Server_msg in
        let module C = Server_msg.Control in
        let control e = Tail.extend t.tail (S.Control e) in
        t.am_listening <- true;
        let socket =
          Socket.listen t.socket
            ~max_pending_connections:(min 1_000 t.max_clients)
        in
        let warn_thres =
          Int.max 1 (Float.iround_towards_zero_exn
                       (float t.max_clients *. t.warn_free_connections_pct))
        in
        let incr_free_connections () =
          t.free_connections <- t.free_connections + 1;
          Option.iter t.when_free ~f:(fun i ->
            t.when_free <- None;
            Ivar.fill i ());
        in
        let rec loop () =
          match t.when_free with
          | Some i -> upon (Ivar.read i) loop
          | None ->
            Monitor.try_with (fun () -> Socket.accept socket)
            >>> function
            | Error exn ->
              Monitor.send_exn (Monitor.current ()) exn;
              upon (Clock.after wait_after_exn) loop
            | Ok (sock, `Inet (addr, port)) ->
              assert (Socket.getopt sock Socket.Opt.nodelay);
              assert (t.free_connections > 0);
              t.num_accepts <- Int63.succ t.num_accepts;
              t.free_connections <- t.free_connections - 1;
              if t.free_connections = 0 then begin
                t.when_free <- Some (Ivar.create ());
                control (C.Too_many_clients "zero free connections")
              end else if t.free_connections <= warn_thres then begin
                control (C.Almost_full t.free_connections)
              end;
              let fd = Socket.fd sock in
              upon (Fd.close_finished fd) incr_free_connections;
              protect ~f:(fun () -> handle_client t addr port fd) ~finally:loop
        in
        loop ()
      end;
      Tail.collect t.tail
    ;;

    let listen_ignore_errors ?(stop = Deferred.never ()) t =
      let s = Stream.take_until (listen t) stop in
      Stream.filter_map s ~f:(function
      | Server_msg.Control _ -> None
      | Server_msg.Data x -> Some x.Read_result.data)
    ;;

    let create
        ?logfun
        ?(now = Scheduler.cycle_start)
        ?(enforce_unique_remote_name = true)
        ?(is_client_ip_authorized = fun _ -> true)
        ?(warn_when_free_connections_lte_pct = 0.05)
        ?(max_clients = 500) ~listen_port my_name =
      if max_clients > 10_000 || max_clients < 1 then
        raise (Invalid_argument "max_clients must be between 1 and 10,000");
      Deferred.create (fun result ->
        let s = Socket.create Socket.Type.tcp in
        upon (Socket.bind s (Socket.Address.inet_addr_any ~port:listen_port))
          (fun socket ->
            let t =
              { tail = Tail.create ();
                socket;
                am_listening = false;
                logfun;
                connections = Connections.create ();
                is_client_ip_authorized;
                my_name;
                enforce_unique_remote_name;
                warn_free_connections_pct = warn_when_free_connections_lte_pct;
                max_clients;
                free_connections = max_clients;
                when_free = None;
                now;
                num_accepts = Int63.zero }
            in
            Ivar.fill result t));
    ;;

    let port t =
      match Socket.getsockname t.socket with
      | `Inet (_, port) -> port
    ;;
  end

  module Client = struct
    type t = {
      remote_ip : Unix.Inet_addr.t;
      remote_port : int;
      expected_remote_name : Remote_name.t;
      check_remote_name : bool; (* check whether the server's name
                                   matches the expected remote name *)
      logfun : logfun option;
      my_name : My_name.t;
      messages : (Remote_name.t, Recv.t) Client_msg.t Tail.t;
      queue : (Send.t * [`Sent of Time.t | `Dropped] Ivar.t) Queue.t;
      mutable con :
        [ `Disconnected
          | `Connected of Connection.t
          | `Connecting of unit -> unit ];
      mutable trying_to_connect : bool;
      mutable connect_complete : unit Ivar.t;
      mutable ok_to_connect : unit Ivar.t;
      now : unit -> Time.t;
      (* last connection error. None if it has succeeded *)
      mutable last_connect_error : exn option;
    }

    (* sweeks: [try_with_timeout] is used in two places.  We could inline it
       here, and it would make the code clearer. *)
    let raise_after_timeout span f =
      try_with_timeout span f
      >>| function
      | `Ok a -> a
      | `Error e -> raise e
      | `Timeout -> failwith "timeout"
    ;;

    exception Hello_name_is_not_expected_remote_name of string * string with sexp

    exception Disconnected with sexp

    exception Write_error of exn with sexp

    let connect t =
      let module C = Client_msg in
      let module E = Client_msg.Control in
      let reset_ok_to_connect () =
        (* Wait a while before trying again *)
        t.ok_to_connect <- Ivar.create ();
        upon
          (Clock.after wait_after_connect_failure)
          (fun () -> Ivar.fill t.ok_to_connect ());
      in
      match Result.try_with (fun () -> Socket.create Socket.Type.tcp) with
      | Error e ->
        t.con <- `Disconnected;
        reset_ok_to_connect ();
        return (Error e)
      | Ok s ->
        let close_this = ref (`Unconnected_socket s) in
        let close msg =
          match !close_this with
          | `Nothing_already_closed -> ()
          | (`Writer _ | `Unconnected_socket _ | `Active_socket _) as socket_or_writer ->
            close_this := `Nothing_already_closed;
            t.con <- `Disconnected;
            reset_ok_to_connect ();
            Tail.extend t.messages
              (C.Control (E.Disconnect (t.expected_remote_name, Exn.sexp_of_t msg)));
            let close () =
              match socket_or_writer with
              | `Unconnected_socket s -> Unix.close (Socket.fd s)
              | `Active_socket s -> Unix.close (Socket.fd s)
              | `Writer w -> Writer.close w ~force_close:(Clock.after (sec 5.))
            in
            ignore_errors close
        in
        t.con <- `Connecting (fun () -> close Disconnected);
        let address = Socket.Address.inet t.remote_ip ~port:t.remote_port in
        Tail.extend t.messages (C.Control E.Connecting);
        Monitor.try_with (fun () ->
          raise_after_timeout connect_timeout (fun () ->
            Socket.connect s address))
        >>= function
        | Error e ->
          close e;
          return (Error e)
        | Ok s ->
          close_this := `Active_socket s;
          Monitor.try_with (fun () ->
            assert (Socket.getopt s Socket.Opt.nodelay);
            let fd = Socket.fd s in
            let reader = Reader.create fd in
            let writer = Writer.create ~syscall:`Per_cycle fd in
            close_this := `Writer writer;
            Stream.iter (Monitor.errors (Writer.monitor writer))
              ~f:(fun e -> close (Write_error e));
            let my_name = My_name.to_string t.my_name in
            raise_after_timeout negotiate_timeout (fun () ->
              negotiate ~reader ~writer
                ~my_name ~auth_error:(fun _ -> None))
            >>| (function
            | `Eof -> failwith "eof"
            | `Auth_error _ -> assert false
            | `Version_error ->
              failwith "cannot negotiate a common version"
            | `Ok (h, send_version, marshal_fun, unmarshal_fun) ->
              let expected_remote_name =
                Remote_name.to_string t.expected_remote_name
              in
              if t.check_remote_name
                && h.Hello.name <> expected_remote_name
              then
                raise (Hello_name_is_not_expected_remote_name
                         (h.Hello.name, expected_remote_name))
              else begin
                let name = Remote_name.of_string h.Hello.name in
                let con =
                  { Connection.
                    writer;
                    reader;
                    marshal_fun;
                    unmarshal_fun;
                    send_version;
                    name;
                    kill = (fun () -> close (Failure "")) }
                in
                Tail.extend t.messages (C.Control (E.Connect name));
                t.con <- `Connected con;
                handle_incoming
                  ~logfun:t.logfun ~remote_name:name
                  ~ip:(Unix.Inet_addr.to_string t.remote_ip)
                  ~con
                  ~extend_disconnect:(fun n e ->
                    Tail.extend t.messages
                      (C.Control (E.Disconnect (n, Exn.sexp_of_t e))))
                  ~extend_parse_error:(fun n e ->
                    Tail.extend t.messages
                      (C.Control (E.Parse_error (n, e))))
                  ~extend_data:(fun x ->
                    Tail.extend t.messages (C.Data x))
              end))
          >>| function
          | Error e -> close e; Error e
          | Ok x -> Ok x
    ;;

    let listen t = Tail.collect t.messages

    let listen_ignore_errors ?(stop = Deferred.never ()) t =
      let s = Stream.take_until (listen t) stop in
      Stream.filter_map s ~f:(function
      | Client_msg.Control _ -> None
      | Client_msg.Data x -> Some x.Read_result.data)

    let internal_send t d =
      match t.con with
      | `Disconnected
      | `Connecting _ -> return `Dropped
      | `Connected con ->
        send ~logfun:t.logfun ~name:t.expected_remote_name ~now:t.now con d

    let send_q t =
      Queue.iter t.queue ~f:(fun (d, i) ->
        upon (internal_send t d) (Ivar.fill i));
      Queue.clear t.queue

    let is_connected t =
      match t.con with
      | `Disconnected
      | `Connecting _ -> false
      | `Connected _ -> true

    let try_connect t =
      assert (not (is_connected t));
      if not t.trying_to_connect then begin
        t.trying_to_connect <- true;
        t.connect_complete <- Ivar.create ();
        let rec loop () =
          upon (Ivar.read t.ok_to_connect) (fun () ->
            upon (connect t) (function
            | Error e ->
              t.last_connect_error <- Some e;
              (* the connect failed, toss everything *)
              Queue.iter t.queue ~f:(fun (_, i) -> Ivar.fill i `Dropped);
              Queue.clear t.queue;
              loop ()
            | Ok () ->
              t.last_connect_error <- None;
              t.trying_to_connect <- false;
              Ivar.fill t.connect_complete ();
              send_q t))
        in
        loop ()
      end

    let send =
      let push t d =
        let i = Ivar.create () in
        Queue.enqueue t.queue (d, i);
        Ivar.read i
      in
      fun t d ->
        match t.con with
        | `Connecting _ -> push t d
        | `Disconnected ->
          let flush = push t d in
          try_connect t;
          flush
        | `Connected _ ->
          if Queue.is_empty t.queue then
            internal_send t d
          else begin
            let flush = push t d in
            send_q t;
            flush
          end

    let close_connection t =
      match t.con with
      | `Disconnected -> ()
      | `Connecting kill -> kill ()
      | `Connected con -> con.Connection.kill ()

    let connect t =
      match t.con with
      | `Connecting _
      | `Connected _ ->
        Ivar.read t.connect_complete
      | `Disconnected ->
        try_connect t;
        Ivar.read t.connect_complete

    let send_ignore_errors t d = ignore (send t d)

    let state t =
      match t.con with
      | `Disconnected -> `Disconnected
      | `Connecting _ -> `Connecting
      | `Connected _ -> `Connected

    let create
        ?logfun
        ?(now = Scheduler.cycle_start)
        ?(check_remote_name = true)
        ~ip ~port ~expected_remote_name my_name =
      return
        { remote_ip = Unix.Inet_addr.of_string ip;
          remote_port = port;
          logfun;
          expected_remote_name;
          check_remote_name;
          my_name;
          queue = Queue.create ();
          messages = Tail.create ();
          con = `Disconnected;
          connect_complete = Ivar.create ();
          ok_to_connect = (let i = Ivar.create () in Ivar.fill i (); i);
          trying_to_connect = false;
          now;
          last_connect_error = None;
        }

    let last_connect_error t = t.last_connect_error
  end
    end

(** Helpers to make your types Datumable if they are binable. Works with up
    to 5 versions (easily extensible to more) *)
module Datumable_of_binable = struct
  module type T = sig type t end
  module type T_bin = sig type t with bin_io end

  module V (V : T) (T : T) = struct
    module type S = sig
      val of_v : V.t -> T.t option
      val to_v : T.t -> V.t option
    end
  end

  module Make_datumable5
    (Versions : Versions)
    (T : T)
    (V1 : T_bin)
    (V2 : T_bin)
    (V3 : T_bin)
    (V4 : T_bin)
    (V5 : T_bin)
    (V1_cvt : V(V1)(T).S)
    (V2_cvt : V(V2)(T).S)
    (V3_cvt : V(V3)(T).S)
    (V4_cvt : V(V4)(T).S)
    (V5_cvt : V(V5)(T).S)
    : Datumable with type datum = T.t =
  struct
    type t = T.t
    type datum = t

    include Versions
    let () = assert (low_version <= prod_version
                     && prod_version <= test_version
                     && test_version <= Version.add low_version 5)

    let alloc = bigsubstring_allocator ()

    let marshal ~to_v ~bin_size_t ~bin_write_t t =
      match to_v t with
      | None -> None
      | Some v ->
        let length = bin_size_t v in
        let bss = alloc length in
        let start = Bigsubstring.pos bss in
        let pos = bin_write_t (Bigsubstring.base bss) ~pos:start v in
        if pos - start <> length then
          failwithf "marshal failure: %d - %d <> %d" pos start length ();
        Some bss
    ;;

    let unmarshal ~of_v ~bin_read_t bss =
      let start = Bigsubstring.pos bss in
      let pos_ref = ref start in
      let result = bin_read_t (Bigsubstring.base bss) ~pos_ref in
      let length = Bigsubstring.length bss in
      if !pos_ref - start <> length then
        failwithf "unmarshal failure: %d - %d <> %d" !pos_ref start length ();
      of_v result
    ;;

    module F (VN : T_bin) (C : V(VN)(T).S) = struct
      let f =
        (fun t -> marshal ~to_v:C.to_v ~bin_size_t:VN.bin_size_t
          ~bin_write_t:VN.bin_write_t t),
        (fun bss -> unmarshal ~of_v:C.of_v ~bin_read_t:VN.bin_read_t bss)
    end
    module F1 = F(V1)(V1_cvt)
    module F2 = F(V2)(V2_cvt)
    module F3 = F(V3)(V3_cvt)
    module F4 = F(V4)(V4_cvt)
    module F5 = F(V5)(V5_cvt)

    let funs = [| F1.f; F2.f; F3.f; F4.f; F5.f |]

    let lookup version =
      Result.try_with (fun () ->
        funs.(Version.to_int version - Version.to_int low_version))
    ;;

    let lookup_marshal_fun version = Result.map (lookup version) ~f:fst
    let lookup_unmarshal_fun version = Result.map (lookup version) ~f:snd
  end

  module type Pre_versions = sig
    val low_version : Version.t
    val prod_version : Version.t
  end

  module Five_versions
    (Versions : Pre_versions)
    (T : T)
    (V1 : T_bin)
    (V2 : T_bin)
    (V3 : T_bin)
    (V4 : T_bin)
    (V5 : T_bin)
    (V1_cvt : V(V1)(T).S)
    (V2_cvt : V(V2)(T).S)
    (V3_cvt : V(V3)(T).S)
    (V4_cvt : V(V4)(T).S)
    (V5_cvt : V(V5)(T).S)
    : Datumable with type datum = T.t =
    Make_datumable5
      (struct
        include Versions
        let test_version = Version.add low_version 4
       end)
      (T)
      (V1)(V2)(V3)(V4)(V5)
      (V1_cvt)(V2_cvt)(V3_cvt)(V4_cvt)(V5_cvt)
  ;;

  module Four_versions
    (Versions : Pre_versions)
    (T : T)
    (V1 : T_bin)
    (V2 : T_bin)
    (V3 : T_bin)
    (V4 : T_bin)
    (V1_cvt : V(V1)(T).S)
    (V2_cvt : V(V2)(T).S)
    (V3_cvt : V(V3)(T).S)
    (V4_cvt : V(V4)(T).S)
    : Datumable with type datum = T.t =
    Make_datumable5
      (struct
        include Versions
        let test_version = Version.add low_version 3
       end)
      (T)
      (V1)(V2)(V3)(V4)(V4)
      (V1_cvt)(V2_cvt)(V3_cvt)(V4_cvt)(V4_cvt)
  ;;

  module Three_versions
    (Versions : Pre_versions)
    (T : T)
    (V1 : T_bin)
    (V2 : T_bin)
    (V3 : T_bin)
    (V1_cvt : V(V1)(T).S)
    (V2_cvt : V(V2)(T).S)
    (V3_cvt : V(V3)(T).S)
    : Datumable with type datum = T.t =
    Make_datumable5
      (struct
        include Versions
        let test_version = Version.add low_version 2
       end)
      (T)
      (V1)(V2)(V3)(V3)(V3)
      (V1_cvt)(V2_cvt)(V3_cvt)(V3_cvt)(V3_cvt)
  ;;

  module Two_versions
    (Versions : Pre_versions)
    (T : T)
    (V1 : T_bin)
    (V2 : T_bin)
    (V1_cvt : V(V1)(T).S)
    (V2_cvt : V(V2)(T).S)
    : Datumable with type datum = T.t =
    Make_datumable5
      (struct
        include Versions
        let test_version = Version.add low_version 1
       end)
      (T)
      (V1)(V2)(V2)(V2)(V2)
      (V1_cvt)(V2_cvt)(V2_cvt)(V2_cvt)(V2_cvt)
  ;;

  module One_version
    (Versions : Pre_versions)
    (T : T)
    (V1 : T_bin)
    (V1_cvt : V(V1)(T).S)
    : Datumable with type datum = T.t =
    Make_datumable5
      (struct
        include Versions
        let test_version = low_version
       end)
      (T)
      (V1)(V1)(V1)(V1)(V1)
      (V1_cvt)(V1_cvt)(V1_cvt)(V1_cvt)(V1_cvt)
  ;;
end
