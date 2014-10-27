
open Core.Std
open Import

include Versioned_typed_tcp_intf

exception Bigsubstring_allocator_got_invalid_requested_size of int with sexp

let bigsubstring_allocator ?(initial_size = 512) () =
  let buf = ref (Bigstring.create initial_size) in
  fun requested_size ->
    if requested_size < 1 then
      raise (Bigsubstring_allocator_got_invalid_requested_size requested_size);
    if requested_size > Bigstring.length !buf then
      buf := (Bigstring.create
                (Int.max requested_size (2 * Bigstring.length !buf)));
    Bigsubstring.create !buf ~pos:0 ~len:requested_size
;;

let protocol_version : [ `Prod | `Test ] ref = ref `Test

module Dont_care_about_mode = struct
  type t = Dont_care_about_mode with bin_io, sexp


  let current () = Dont_care_about_mode
  let (=) (a : t) b = a = b
end

module Repeater_hook = struct
  type ('msg, 'conn) t =
    { on_error : Repeater_error.t -> unit;
      on_data : msg:'msg -> raw_msg:Bigsubstring.t -> conn:'conn -> unit;
    }
end

(* helper types to make signatures clearer *)
type 'a wo_my_name =
  < send : 'send;
    recv : 'recv;
    remote_name : 'remote_name >
  constraint 'a =
    < send : 'send;
      recv : 'recv;
      my_name : 'my_name;
      remote_name : 'remote_name >

type 'a flipped =
  < send : 'recv;
    recv : 'send;
    my_name     : 'remote_name;
    remote_name : 'my_name;
  >
  constraint 'a =
    < send : 'send;
      recv : 'recv;
      my_name : 'my_name;
      remote_name : 'remote_name >

module Make (Z : Arg) = struct
  include Z

  module Constants = struct
    let negotiate_timeout = sec 10.
    let wait_after_exn = sec 0.5

    let wait_after_connect_failure = sec 4.
    let connect_timeout = sec 10.
  end

  open Constants

  type 'a logfun =
    [ `Recv of 'recv | `Send of 'send ]
    -> 'remote_name
    -> time_sent_received:Time.t
    -> unit
    constraint 'a = < send : 'send; recv : 'recv; remote_name : 'remote_name >

  (* mstanojevic: note that Hello.t contains Mode, which means that we
     can't ever change the Mode type! *)
  module Hello = struct
    type t = {
      name : string;
      mode : Mode.t;
      send_version : Version.t;
      recv_version : Version.t;
      credentials : string;
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
    type 'a t = {
      writer : Writer.t;
      reader : Reader.t;
      marshal_fun : 'send marshal_fun;
      unmarshal_fun : 'recv unmarshal_fun;
      send_version : Version.t;
      my_name : 'my_name;
      remote_name : 'remote_name;
      kill: unit -> unit;
    } constraint 'a = < send : 'send;
                        recv : 'recv;
                        my_name : 'my_name;
                        remote_name : 'remote_name >

    let kill t = t.kill ()
  end

  let try_with = Monitor.try_with

  let ignore_errors f = don't_wait_for (try_with f >>| ignore)

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

  let negotiate (type r) (type s)  ~reader ~writer ~my_name ~credentials ~auth_error ~recv ~send =
    let module Recv = (val recv : Datum with type t = r) in
    let module Send = (val send : Datum with type t = s) in
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
        ~credentials
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
  exception Unconsumed_data of string with sexp

  let dummy_bigsubstring = Bigsubstring.of_string ""

  let handle_incoming
        ~logfun ~remote_name ~ip ~con ~extend_data_needs_raw
        (* functions to extend the tail (with various messages) *)
        ~extend_disconnect
        ~extend_parse_error
        ~extend_data
    =
    let module C = Connection in
    Writer.set_raise_when_consumer_leaves con.C.writer false;
    upon (Writer.consumer_left con.C.writer) con.C.kill;
    (* Benign errors like EPIPE and ECONNRESET will not be raised, so we are left with
       only serious errors ("man 2 write" lists EBADF, EFAULT, EFBIG, EINVAL, EIO, ENOSPC,
       all of which point to either a bug in async or to a pretty bad state of the
       system).  We close the connection and propagate the error up. *)
    Stream.iter (Monitor.detach_and_get_error_stream (Writer.monitor con.C.writer)) ~f:(fun e ->
      con.C.kill ();
      (* As opposed to [raise], this will continue propagating subsequent exceptions. *)
      (* This can't lead to an infinite loop because con.C.writer is not exposed *)
      Monitor.send_exn (Monitor.current ()) e);
    let extend ~time_sent ~time_received data raw =
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
        raw
    in
    let len_len = 8 in
    let pos_ref = ref 0 in
    let rec handle_chunk buf ~consumed ~pos ~len =
      if len = 0 then
        return `Continue
      else if len_len > len then
        return (`Consumed (consumed, `Need len_len))
      else begin
        pos_ref := pos;
        (* header has two fields, [time_stamp] which is 8 bytes over bin_io, and
           [body_length] which is variable length encoded int that can in theory be 1 to 8
           bytes over bin_io, but in practice it doesn't take more than 3 bytes *)
        let hdr_len = Bin_prot.Read.bin_read_int_64bit buf ~pos_ref in
        if len_len + hdr_len > len then
          return (`Consumed (consumed, `Need (len_len + hdr_len)))
        else begin
          let hdr =
            try
              Ok (Message_header.bin_reader_t.Bin_prot.Type_class.read buf ~pos_ref)
            with
              exn -> Error exn
          in
          match hdr with
          | Error exn ->
            return (`Stop exn)
          | Ok hdr ->
            let body_len = hdr.Message_header.body_length in
            let msg_len = len_len + hdr_len + body_len in
            if msg_len > len then
              return (`Consumed (consumed, `Need msg_len))
            else begin
              let body = Bigsubstring.create buf ~pos:(!pos_ref) ~len:body_len in
              begin match try Ok (con.C.unmarshal_fun body) with ex -> Error ex with
              | Error exn ->
                extend_parse_error remote_name (Exn.to_string exn);
              | Ok None -> ()
              | Ok (Some msg) ->
                let time_received = Reader.last_read_time con.C.reader in
                let time_sent = hdr.Message_header.time_stamp in
                let raw_msg =
                  if extend_data_needs_raw then
                    Bigsubstring.create buf ~pos ~len:msg_len
                  else
                    (* a performance hack: this isn't really used downstream *)
                    dummy_bigsubstring
                in
                extend ~time_received ~time_sent msg raw_msg
              end;
              handle_chunk
                buf
                ~consumed:(consumed + msg_len)
                ~pos:(pos + msg_len)
                ~len:(len - msg_len)
            end
        end
      end
    in
    Reader.read_one_chunk_at_a_time con.C.reader
      ~handle_chunk:(handle_chunk ~consumed:0)
    >>> (fun result ->
    con.C.kill ();
    let exn =
      match result with
      | `Eof -> Eof
      | `Stopped exn -> exn
      | `Eof_with_unconsumed_data data -> (Unconsumed_data data)
    in
    extend_disconnect remote_name exn)
  ;;

  module Server = struct

    type dir = < send : To_client_msg.t;
                 recv : To_server_msg.t;
                 my_name : Server_name.t;
                 remote_name : Client_name.t >

    module Connection = struct
      type t = dir Connection.t

      let kill = Connection.kill
    end

    type nonrec logfun = dir wo_my_name logfun

    module Connections : sig
      type t
      val create : unit -> t
      val mem : t -> Client_name.t -> bool
      val find : t -> Client_name.t -> Connection.t option
      val add : t -> name:Client_name.t -> conn:Connection.t -> unit
      val remove : t -> Client_name.t -> unit

      val fold
        : t
        -> init:'a
        -> f:(name:Client_name.t -> conn:Connection.t -> 'a-> 'a)
        -> 'a

      val send_to_all
        :  t
        -> logfun:logfun option
        -> now:(unit -> Time.t)
        -> To_client_msg.t
        -> [ `Sent (** sent successfuly to all clients *)
           | `Dropped (** not sent successfully to any client *)
           | `Partial_success (** sent to some clients *)
           ] Deferred.t

      val send_to_all_ignore_errors : t
        -> logfun:logfun option
        -> now:(unit -> Time.t)
        -> To_client_msg.t
        -> unit

      val send_to_some : t
        -> logfun:logfun option
        -> now:(unit -> Time.t)
        -> To_client_msg.t
        -> Client_name.t list
        -> [ `Sent (** sent successfuly to all clients *)
           | `Dropped (** not sent successfully to any client *)
           | `Partial_success (** sent to some clients *)] Deferred.t

      val send_to_some_ignore_errors : t
        -> logfun:logfun option
        -> now:(unit -> Time.t)
        -> To_client_msg.t
        -> Client_name.t list
        -> unit
    end = struct

      module C = Connection

      type t = {
        by_name : (C.t Bag.t * C.t Bag.Elt.t) Client_name.Table.t;
        by_send_version : (C.t Bag.t * To_client_msg.t marshal_fun) Version.Table.t;
      }

      let create () =
        { by_name = Client_name.Table.create ();
          by_send_version =
            Version.Table.create
              ~size:(Version.to_int To_client_msg.test_version) ();
        }
      ;;

      let fold t ~init ~f =
        Hashtbl.fold t.by_name ~init ~f:(fun ~key ~data:(_, bag_elt) acc ->
          f ~name:key ~conn:(Bag.Elt.value bag_elt) acc)
      ;;

      let mem t name = Hashtbl.mem t.by_name name

      let add t ~name ~(conn : Connection.t) =
        let bag, _marshal_fun =
          Hashtbl.find_or_add t.by_send_version conn.send_version
            ~default:(fun () -> Bag.create (), conn.marshal_fun)
        in
        let bag_elt = Bag.add bag conn in
        Hashtbl.set t.by_name ~key:name ~data:(bag, bag_elt);
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

      let send_to_some'' d ~logfun ~now by_version is_bag_empty iter_bag =
        let module C = Connection in
        let module H = Message_header in
        let now = now () in
        let res =
          Hashtbl.fold by_version ~init:`Init
            ~f:(fun ~key:_ ~data:(bag, marshal_fun) acc ->
              if not (is_bag_empty bag) then begin
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
                        fun (conn : C.t) ->
                          wrap_write_bin_prot
                            ~sexp:H.sexp_of_t ~tc:H.bin_t.Bin_prot.Type_class.writer
                            ~writer:conn.writer
                            ~name:"send" hdr;
                          Writer.schedule_bigstring conn.writer to_schedule
                      end
                      else begin
                        fun conn ->
                          send_raw ~writer:conn.writer ~hdr ~msg;
                      end
                    in
                    iter_bag bag ~f:(fun conn ->
                      send conn;
                      maybe_log ~logfun ~now ~name:conn.remote_name d);
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

      let send_to_all' t d ~logfun ~now =
        send_to_some'' d ~logfun ~now t.by_send_version Bag.is_empty Bag.iter
      ;;

      let send_to_all t ~logfun ~now d =
        let res = send_to_all' t d ~logfun ~now in
        Deferred.all_unit (fold t ~init:[] ~f:(fun ~name:_ ~conn acc ->
          Writer.flushed conn.writer :: acc))
        >>| fun () -> res
      ;;

      let send_to_all_ignore_errors t ~logfun ~now d =
        ignore (send_to_all' t d ~logfun ~now : [`Partial_success | `Dropped | `Sent])
      ;;

      let send_to_some' t d ~logfun ~now names =
        let tbl = Version.Table.create ~size:(Version.to_int To_client_msg.test_version) () in
        let all_names_found =
          List.fold names ~init:true ~f:(fun acc name ->
            let conn = find t name in
            match conn with
            | None -> false
            | Some conn ->
              let version = conn.send_version in
              let conns =
                match Hashtbl.find tbl version with
                | Some (conns, _marshal_fun) -> conns
                | None -> []
              in
              Hashtbl.set tbl ~key:version ~data:(conn::conns, conn.marshal_fun);
              acc)
        in
        if Hashtbl.is_empty tbl then `Dropped
        else
          let res = send_to_some'' d ~logfun ~now tbl List.is_empty List.iter in
          if all_names_found then
            res
          else
            match res with
            | `Sent | `Partial_success -> `Partial_success
            | `Dropped -> `Dropped
      ;;

      let send_to_some t ~logfun ~now d names =
        let res = send_to_some' t d ~logfun ~now names in
        Deferred.all_unit (fold t ~init:[] ~f:(fun ~name:_ ~conn acc ->
          Writer.flushed conn.writer :: acc))
        >>| fun () -> res
      ;;

      let send_to_some_ignore_errors t ~logfun ~now d names =
        ignore (send_to_some' t d ~logfun ~now names : [`Partial_success | `Dropped | `Sent])
      ;;
    end

    type t = {
      tail : (Client_name.t, To_server_msg.t) Server_msg.t Tail.t;
      logfun : logfun option;
      connections : Connections.t;
      mutable am_listening : bool;
      socket : ([ `Bound ], Socket.Address.Inet.t) Socket.t;
      warn_free_connections_pct: float;
      mutable free_connections: int;
      mutable when_free: unit Ivar.t option;
      max_clients: int;
      is_client_ip_authorized : string -> bool;
      my_name : Server_name.t;
      enforce_unique_remote_name : bool;
      now : unit -> Time.t;
      mutable num_accepts : Int63.t;
      credentials : string;
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
            (choose [ choice (Writer.flushed conn.writer)
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

    let shutdown t =
      Fd.close (Socket.fd t.socket)
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

    let send_to_some t d names =
      Connections.send_to_some t.connections d ~now:t.now ~logfun:t.logfun names
    ;;

    let send_to_some_ignore_errors t d names =
      Connections.send_to_some_ignore_errors t.connections d ~now:t.now ~logfun:t.logfun names
    ;;

    let client_is_authorized t ip =
      t.is_client_ip_authorized (Unix.Inet_addr.to_string ip)
    ;;

    let close t name =
      Option.iter (Connections.find t.connections name) ~f:Connection.kill
    ;;

    let handle_client t addr port fd ~create_repeater_hook =
      let module S = Server_msg in
      let module C = Server_msg.Control in
      let control e = Tail.extend t.tail (S.Control e) in
      let close () = don't_wait_for (Deferred.ignore (Unix.close fd)) in
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
        Stream.iter (Monitor.detach_and_get_error_stream (Writer.monitor w))
          (* We don't report the error to the tail here because this is before
             protocol negotiation.  Once protocol negotiation happens,
             [handle_incoming] will report any errors to the tail.
          *)
          ~f:(fun _ -> !kill ());
        let res =
          try_with_timeout negotiate_timeout (fun () ->
            negotiate
              ~my_name:(Server_name.to_string t.my_name)
              ~credentials:t.credentials
              ~reader:r ~writer:w
              ~send:(module To_client_msg)
              ~recv:(module To_server_msg)
              ~auth_error:(fun h ->
                if not (Mode.(=) h.Hello.mode (Mode.current ())) then
                  Some (C.Wrong_mode (Client_name.of_string h.Hello.name))
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
          match Result.try_with (fun () -> Client_name.of_string name) with
          | Error exn ->
            die_error
              (C.Protocol_error
                 (sprintf "error constructing name: %s, error: %s"
                    name (Exn.to_string exn)))
          | Ok client_name ->
            let module T = Client_name.Table in
            if Connections.mem t.connections client_name then
              die_error (C.Duplicate client_name)
            else begin
              let close =
                lazy
                  (Lazy.force close;
                   Connections.remove t.connections client_name)
              in
              kill := (fun () -> Lazy.force close);
              let (conn : Connection.t ) =
                { writer = w;
                  reader = r;
                  unmarshal_fun;
                  marshal_fun;
                  send_version;
                  my_name = t.my_name;
                  remote_name = client_name;
                  kill = !kill }
              in
              Connections.add t.connections ~name:client_name ~conn;
              Tail.extend t.tail
                (Control (Connect (client_name, `credentials h.Hello.credentials)));
              let ip = Unix.Inet_addr.to_string addr in
              match create_repeater_hook with
              | Some create_repeater_hook ->
                upon (create_repeater_hook client_name ~repeater_to_client_conn:conn)
                  (function
                    | Error e ->
                      die_error (C.Protocol_error (Error.to_string_hum e))
                    | Ok (hook : (_,_) Repeater_hook.t) ->
                      handle_incoming
                        ~logfun:t.logfun ~remote_name:client_name
                        ~ip ~con:conn
                        ~extend_data_needs_raw:true
                        ~extend_disconnect:
                          (fun _ exn -> hook.on_error (Disconnect (Error.of_exn exn)))
                        ~extend_parse_error:(fun _ msg -> hook.on_error (Parse_error msg))
                        ~extend_data:(fun msg raw_msg ->
                          hook.on_data ~msg ~raw_msg ~conn))
              | None ->
                handle_incoming
                  ~logfun:t.logfun ~remote_name:client_name
                  ~ip ~con:conn
                  ~extend_data_needs_raw:false
                  ~extend_disconnect:(fun n e ->
                    Tail.extend t.tail (S.Control (C.Disconnect (n, Exn.sexp_of_t e))))
                  ~extend_parse_error:(fun n e ->
                    Tail.extend t.tail (S.Control (C.Parse_error (n, e))))
                  ~extend_data:(fun x _ -> Tail.extend t.tail (S.Data x))
            end)
      end
    ;;

    let listen' t ~create_repeater_hook =
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
          Int.max 1 (Float.iround_exn ~dir:`Zero
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
            | Ok `Socket_closed -> ()
            | Ok (`Ok (sock, `Inet (addr, port))) ->
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
              protect
                ~f:(fun () -> handle_client t addr port fd ~create_repeater_hook)
                ~finally:loop
        in
        loop ()
      end;
      Tail.collect t.tail
    ;;

    let listen t = listen' t ~create_repeater_hook:None

    let listen_ignore_errors ?(stop = Deferred.never ()) t =
      let s = Stream.take_until (listen t) stop in
      Stream.filter_map_deprecated s ~f:(function
      | Server_msg.Control _ -> None
      | Server_msg.Data x -> Some x.Read_result.data)
    ;;

    let create'
        ?logfun
        ?(now = Scheduler.cycle_start)
        ?(enforce_unique_remote_name = true)
        ?(is_client_ip_authorized = fun _ -> true)
        ?(warn_when_free_connections_lte_pct = 0.05)
        ?(max_clients = 500)
        ?(credentials = "")
        ~listen_port
        my_name =
      if max_clients > 10_000 || max_clients < 1 then
        raise (Invalid_argument "max_clients must be between 1 and 10,000");
      Deferred.create (fun result ->
        let s = Socket.create Socket.Type.tcp in
        upon (Socket.bind s (Socket.Address.Inet.create_bind_any ~port:listen_port))
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
                credentials;
                num_accepts = Int63.zero }
            in
            Ivar.fill result t));
    ;;

    let create = create' ?credentials:None

    let port t =
      match Socket.getsockname t.socket with
      | `Inet (_, port) -> port
    ;;

    let client_send_version t name =
      match Connections.find t.connections name with
      | None -> None
      | Some conn -> Some conn.send_version
    ;;

    let client_is_connected t name = Connections.mem t.connections name
  end

  module Client = struct

    type dir = < send : To_server_msg.t;
                 recv : To_client_msg.t;
                 my_name : Client_name.t;
                 remote_name : Server_name.t >

    type nonrec logfun = dir wo_my_name logfun

    module Connection = struct
      type t = dir Connection.t
    end

    type t = {
      remote_ip : Unix.Inet_addr.t;
      remote_port : int;
      expected_remote_name : Server_name.t;
      check_remote_name : bool; (* check whether the server's name
                                   matches the expected remote name *)
      logfun : logfun option;
      my_name : Client_name.t;
      messages : (Server_name.t, To_client_msg.t) Client_msg.t Tail.t;
      queue : (To_server_msg.t * [ `Sent of Time.t | `Dropped] Ivar.t) Queue.t;
      mutable con :
        [ `Disconnected
        | `Connected of Connection.t
        | `Connecting of unit -> unit ];
      mutable trying_to_connect : [`No | `Yes of unit Ivar.t];
      mutable connect_complete : unit Ivar.t;
      mutable ok_to_connect : unit Ivar.t;
      now : unit -> Time.t;
      (* last connection error. None if it has succeeded *)
      mutable last_connect_error : exn option;
      repeater_hook : ((Server_name.t, To_client_msg.t) Read_result.t, Connection.t)
                        Repeater_hook.t option;
      credentials : string;
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

    let connect_internal t =
      assert
        (match t.trying_to_connect with
        | `Yes _ -> true
        | `No    -> false);
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
        let address = Socket.Address.Inet.create t.remote_ip ~port:t.remote_port in
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
            Stream.iter (Monitor.detach_and_get_error_stream (Writer.monitor writer))
              ~f:(fun e -> close (Write_error e));
            let my_name = Client_name.to_string t.my_name in
            raise_after_timeout negotiate_timeout (fun () ->
              negotiate ~reader ~writer
                ~send:(module To_server_msg)
                ~recv:(module To_client_msg)
                ~credentials:t.credentials
                ~my_name ~auth_error:(fun _ -> None))
            >>| (function
            | `Eof -> failwith "eof"
            | `Auth_error _ -> assert false
            | `Version_error ->
              failwith "cannot negotiate a common version"
            | `Ok (h, send_version, marshal_fun, unmarshal_fun) ->
              let expected_remote_name =
                Server_name.to_string t.expected_remote_name
              in
              if t.check_remote_name
                && h.Hello.name <> expected_remote_name
              then
                raise (Hello_name_is_not_expected_remote_name
                         (h.Hello.name, expected_remote_name))
              else begin
                let server_name = Server_name.of_string h.Hello.name in
                let (con : Connection.t )=
                  { writer;
                    reader;
                    marshal_fun;
                    unmarshal_fun;
                    send_version;
                    remote_name = server_name;
                    my_name = t.my_name;
                    kill = (fun () -> close (Failure "connection was killed")) }
                in
                t.con <- `Connected con;
                Tail.extend t.messages
                  (Control (Connect (server_name, `credentials h.Hello.credentials)));
                let ip = Unix.Inet_addr.to_string t.remote_ip in
                match t.repeater_hook with
                | Some hook ->
                  handle_incoming
                    ~logfun:t.logfun
                    ~remote_name:server_name
                    ~ip ~con
                    ~extend_data_needs_raw:true
                    ~extend_disconnect:
                      (fun _ exn -> hook.on_error (Disconnect (Error.of_exn exn)))
                    ~extend_parse_error:(fun _ msg -> hook.on_error (Parse_error msg))
                    ~extend_data:(fun msg raw_msg ->
                      hook.on_data ~msg ~raw_msg ~conn:con);
                | None ->
                  handle_incoming
                    ~logfun:t.logfun ~remote_name:server_name
                    ~ip ~con
                    ~extend_data_needs_raw:false
                    ~extend_disconnect:(fun n e ->
                      Tail.extend t.messages
                        (C.Control (E.Disconnect (n, Exn.sexp_of_t e))))
                    ~extend_parse_error:(fun n e ->
                      Tail.extend t.messages
                        (C.Control (E.Parse_error (n, e))))
                    ~extend_data:(fun x _ ->
                      Tail.extend t.messages (C.Data x))
              end))
          >>| function
          | Error e -> close e; Error e
          | Ok x -> Ok x
    ;;

    let flushed t =
      match t.con with
      | `Disconnected
      | `Connecting _  -> `Flushed
      | `Connected con ->
        if 0 = Writer.bytes_to_write con.writer then
          `Flushed
        else
          `Pending (Writer.flushed_time con.writer)
    ;;

    let listen t = Tail.collect t.messages

    let listen_ignore_errors ?(stop = Deferred.never ()) t =
      let s = Stream.take_until (listen t) stop in
      Stream.filter_map_deprecated s ~f:(function
      | Client_msg.Control _ -> None
      | Client_msg.Data x -> Some x.Read_result.data)

    let internal_send t msg =
      match t.con with
      | `Disconnected | `Connecting _ -> return `Dropped
      | `Connected con ->
        send ~logfun:t.logfun ~name:t.expected_remote_name ~now:t.now con msg

    let send_q t =
      Queue.iter t.queue ~f:(fun (d, i) ->
        upon (internal_send t d) (Ivar.fill i));
      Queue.clear t.queue

    let is_connected t =
      match t.con with
      | `Disconnected
      | `Connecting _ -> false
      | `Connected _ -> true

    let purge_queue t =
      Queue.iter t.queue ~f:(fun (_, i) -> Ivar.fill i `Dropped);
      Queue.clear t.queue;
    ;;

    let try_connect t =
      assert (not (is_connected t));
      match t.trying_to_connect with
      | `Yes _ -> ()
      | `No ->
        let killed = Ivar.create () in
        (* [killed] is private to this instance of the [try_connect] loop.  This is why
           calling [try_connect] again immediately after [close_connection] will result in
           exactly one instance of this loop surviving. *)
        t.trying_to_connect <- `Yes killed;
        t.connect_complete <- Ivar.create ();
        let rec loop () =
          (Deferred.any [Ivar.read t.ok_to_connect;
                         Ivar.read killed])
          >>> (fun () ->
            (* In case when [killed] was filled, we do not do anything here.  The cleanup
               ([purge_queue]) happened in the same async cycle as filling the [killed],
               to avoid a possible (though unlikely) race when [send] immediately follows
               [close_connection]--so that the old instance of this loop does not purge
               the newly sent messages from the queue right before shutting down. *)
            if Ivar.is_empty killed then begin
              upon (connect_internal t) (function
              | Error e ->
                t.last_connect_error <- Some e;
                (* the connect failed, toss everything *)
                purge_queue t;
                loop ()
              | Ok () ->
                (* mstanojevic: note that we can be here even if [killed] is filled. but
                   I think that just means that connection will drop very soon. *)
                t.last_connect_error <- None;
                t.trying_to_connect <- `No;
                Ivar.fill t.connect_complete ();
                send_q t)
            end)
        in
        loop ()
    ;;


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
    ;;

    let close_connection t =
      begin match t.trying_to_connect with
      | `No -> ()
      | `Yes kill ->
        Ivar.fill kill ();
        (* drop the messages now, do not wait until the next async cycle, because a
           [send] might follow *)
        purge_queue t;
        (* Ok to call [try_connect] again on a subsequent [send] *)
        t.trying_to_connect <- `No
      end;
      match t.con with
      | `Disconnected -> ()
      | `Connecting kill -> kill ()
      | `Connected con -> con.kill ()
    ;;

    let connect t =
      match t.con with
      | `Connecting _
      | `Connected _ ->
        Ivar.read t.connect_complete
      | `Disconnected ->
        try_connect t;
        Ivar.read t.connect_complete
    ;;

    let send_ignore_errors t d = ignore (send t d)

    let state t =
      match t.con with
      | `Disconnected -> `Disconnected
      | `Connecting _ -> `Connecting
      | `Connected _ -> `Connected
    ;;

    let create0
        ?logfun
        ?(now = Scheduler.cycle_start)
        ?(check_remote_name = true)
        ?repeater_hook
        ?(credentials = "")
        ~ip ~port ~expected_remote_name my_name =
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
        trying_to_connect = `No;
        now;
        last_connect_error = None;
        repeater_hook;
        credentials;
      }
    ;;

    let create' = create0 ?repeater_hook:None

    let create = create0 ?repeater_hook:None ?credentials:None

    let last_connect_error t = t.last_connect_error
  end
end

module Repeater
         (To_server_msg : Datum)
         (To_client_msg : Datum)
         (Server_name : Name)
         (Client_name : Name)
         (Mode : Mode) =
struct

  module Z = Make (struct
    module To_client_msg = To_client_msg
    module To_server_msg = To_server_msg
    module Client_name = Client_name
    module Server_name = Server_name
    module Mode = Mode
  end)

  module Server = Z.Server

  (* restrict functions from client so that we don't call send functions that can initiate
     connection *)
  module Client = struct
    module C = Z.Client
    type t = C.t
    open C
    let is_connected = is_connected
    let connect = connect
    let close_connection = close_connection
    let last_connect_error = last_connect_error
    let create = create0

    let send_ignore_errors t msg =
      if is_connected t then send_ignore_errors t msg
    ;;
  end

  module Connection = Z.Connection

  type repeater_to_server_side = < send : To_server_msg.t;
                                   recv : To_client_msg.t;
                                   my_name : Client_name.t;
                                   remote_name : Server_name.t >

  type repeater_to_client_side = repeater_to_server_side flipped

  type ('state, 'send, 'recv) filter =
    'recv -> state:'state
    -> client_name:Client_name.t
    -> server_name:Server_name.t
    -> ('send, 'recv) Repeater_hook_result.t

  type t =
    { server : Server.t;
      server_ip : string;
      server_port : int;
      server_name : Server_name.t;
      repeater_name : string;
      clients : Client.t Client_name.Table.t;
    }

  let active_clients t =
    Hashtbl.fold t.clients ~init:[] ~f:(fun ~key:client_name ~data:client acc ->
      if
        Client.is_connected client
        && Server.client_is_connected t.server client_name
      then client_name::acc
      else acc)
  ;;

  let create
        ?is_client_ip_authorized
        ~repeater_name
        ~listen_port
        ~server_ip
        ~server_port
        ~server_name =
    let equal3 a b c = (a = b && b = c) in
    if
      not (equal3
             To_server_msg.low_version
             To_server_msg.prod_version
             To_server_msg.test_version)
      || not (equal3
                To_client_msg.low_version
                To_client_msg.prod_version
                To_client_msg.test_version)
    then begin
      let of_v = Version.to_int in
      failwithf "Cannot create a repeater with multiple versions. \
                 Server: %d/%d/%d Client: %d/%d/%d"
        (of_v To_server_msg.low_version) (of_v To_server_msg.prod_version)
        (of_v To_server_msg.test_version) (of_v To_client_msg.low_version)
        (of_v To_client_msg.prod_version) (of_v To_client_msg.test_version)
        ()
    end;
    Server.create' ?is_client_ip_authorized ~listen_port
      ~credentials:repeater_name
      server_name
    >>| fun server ->
    { server;
      clients = Client_name.Table.create ();
      server_ip;
      server_port;
      server_name;
      repeater_name;
    }
  ;;

  module Connection_side : sig
    type _ t =
      | Repeater_to_server : repeater_to_server_side t
      | Repeater_to_client : repeater_to_client_side t

    val to_poly : _ t -> [ `repeater_to_server | `repeater_to_client ]

    val flip : 'a t -> 'a flipped t

  end = struct

    type _ t =
      | Repeater_to_server : repeater_to_server_side t
      | Repeater_to_client : repeater_to_client_side t

    let to_poly (type a) (t : a t) =
      match t with
      | Repeater_to_server -> `repeater_to_server
      | Repeater_to_client -> `repeater_to_client

    let flip : type a b c d .
      < send :a; recv:b; my_name:c; remote_name:d> t ->
      < send :b; recv:a; my_name:d; remote_name:c> t =
      function
      | Repeater_to_client -> Repeater_to_server
      | Repeater_to_server -> Repeater_to_client
  end

  let send_msg
        msg
        ~(conn_side : 'a Connection_side.t)
        ~(conn : 'a Z.Connection.t)
        ~on_send_error =
    let now = Scheduler.cycle_start in
    match Z.send_no_flush ~logfun:None ~now ~name:conn.my_name conn msg with
    | `Sent -> ()
    | `Not_sent ->
      on_send_error conn_side Repeater_error.Marshaling_error
  ;;

  (* create a [Repeater_hook.t] that is used to filter messages on one side of paired
     connections. The side is indicated with [conn_side] argument. We don't pass in the
     connection on that side, because it will be passed in the [on_data] callback by the
     code in Server or Client modules (depending on the side) and also because we don't
     have it always when the hook is created (as is the case of repeater->server side).
     We do pass the opposite side connection, which we need to be able to efficiently pass
     messages through. Assumption is that Pass_on is the most common result of the
     application level filter. In that case, we just blit the bytes over to the other side
     instead of invoking more complicated send functions in Server and Client.

     Type annotations hopefully reduce some confusion about which side of the paired
     connections we are dealing with and prevent sending messages over the wrong
     connection. *)
  let make_hook
        ~(conn_side:(<send:'send;
                      recv:'recv;
                     my_name:_;remote_name:_> as 'side) Connection_side.t)
        ~(paired_conn:'side flipped Connection.t)
        ~client_name
        ~server_name
        ~app_on_error
        ~(app_msg_filter : ('state, 'send, 'recv) filter)
        ~(state : 'state)
    =
    let on_error' conn_side error =
      app_on_error ~client_name ~server_name ~state
        (Connection_side.to_poly conn_side) error
    in
    let on_send_error = on_error' in
    let send_forward msg =
      send_msg msg ~conn_side:(Connection_side.flip conn_side)
        ~conn:paired_conn
        ~on_send_error
    in
    let send_back (conn : 'side Connection.t) msg =
      send_msg msg ~conn_side ~conn ~on_send_error
    in
    let forward_bytes (raw_msg:Bigsubstring.t) =
      Writer.write_bigsubstring paired_conn.writer raw_msg
    in
    let on_data
          ~msg:{ Read_result.data; _ }
          ~raw_msg
          ~(conn:'a Connection.t) =
      match
        (app_msg_filter data ~state ~client_name ~server_name
        : (_,_) Repeater_hook_result.t)
      with
      | Do_nothing -> ()
      | Send msg -> send_forward msg
      | Pass_on -> forward_bytes raw_msg
      | Pass_on_and_send_back msgs ->
        forward_bytes raw_msg;
        List.iter msgs ~f:(fun msg -> send_back conn msg);
      | Send_back msgs ->
        List.iter msgs ~f:(fun msg -> send_back conn msg);
    in
    let on_error error = on_error' conn_side error in
    { Repeater_hook.
      on_error;
      on_data;
    }
  ;;

  (* [start ~on_connect ~to_server_msg_filter ~to_client_msg_filter ~on_error] uses hooks
     available in Server and Client modules to establish an efficient way to filter
     messages between a client and a server.

     After a client establishes a connection to the repeater (acting in a Server.t role),
     the server code will call [create_repeater_hook]. [create_repeater_hook] can return
     [client<->repeater] side hook that will be then used by the Server code to filter
     messages.

     We construct [create_repeater_hook] in such a way that as a side effect it creates a
     connection from the repeater to the real server (for this connection, repeater is in
     a Client.t role) and the [Repeater_hook.t] for that side. Both hooks that are created
     know about the other side's connection, which we need to efficiently transfer
     messages. (see comment in [make_hook] above)

     This is a rather convoluted way of creating hooks but it is done in this way to
     minimize changes to Server and Client code and because there is a inherent recursive
     dependency between connections and their hooks.

     Note that hooks can refer to connections that are closed. This fine because the code
     doesn't allow a new pair of connections to be established until both connections are
     cleaned up. Any message sent over closed connections is just dropped. As documented
     in the mli, it is up to applications that use this module to handle disconnects and
     close the other side.
  *)
  let start t
        ~(on_connect : Client_name.t -> 'state Or_error.t)
        ~(to_server_msg_filter : ('state, To_client_msg.t, To_server_msg.t) filter)
        ~(to_client_msg_filter : ('state, To_server_msg.t, To_client_msg.t) filter)
        ~(on_error : client_name:Client_name.t -> server_name:Server_name.t ->
          state:'state ->
          [ `repeater_to_server | `repeater_to_client ] -> Repeater_error.t -> unit)
    =
    let server_name = t.server_name in
    let create_repeater_hook
          client_name ~(repeater_to_client_conn : Server.Connection.t)
      =
      (* first allow the application to setup the connection state or to refuse the
         client *)
      match on_connect client_name with
      | Error _ as e -> return e
      | Ok state ->
        (* This hook kicks in when the server sends a message to the client via the
           repeater (but in response the hook might end up sending messages both to the
           server and to the client). *)
        let server_to_repeater_hook =
          make_hook
            ~conn_side:Repeater_to_server
            ~paired_conn:repeater_to_client_conn
            ~client_name
            ~server_name
            ~app_on_error:on_error
            ~app_msg_filter:to_client_msg_filter
            ~state
        in
        let new_client =
          Client.create
            ~ip:t.server_ip
            ~port:t.server_port
            ~expected_remote_name:server_name
            ~repeater_hook:server_to_repeater_hook
            ~credentials:t.repeater_name
            client_name
        in
        (* connect repeater to the real server *)
        Clock.with_timeout
          Z.Constants.connect_timeout
          (Client.connect new_client)
        >>| fun conn_res ->
        let res =
          match conn_res with
          | `Timeout ->
            Client.close_connection new_client; (* so we stop retrying *)
            Or_error.error "timed out trying to connect to server"
              (server_name, `last_connect_error (Client.last_connect_error new_client))
              <:sexp_of<Server_name.t * [`last_connect_error of Exn.t option]>>
          | `Result () ->
            match new_client.con with
            | `Disconnected
            | `Connecting _
              ->
              Or_error.error "connection was dropped very quickly after creation"
                (client_name, server_name) <:sexp_of<Client_name.t * Server_name.t>>
            | `Connected repeater_to_server_conn ->
              Hashtbl.set t.clients ~key:client_name ~data:new_client;
              (* This hook kicks in when the client sends a message to the server via the
                 repeater (but in response the hook might end up sending messages both to
                 the server and to the client). *)
              let client_to_repeater_hook =
                make_hook
                  ~conn_side:Repeater_to_client
                  ~paired_conn:repeater_to_server_conn
                  ~client_name
                  ~server_name
                  ~app_on_error:on_error
                  ~app_msg_filter:to_server_msg_filter
                  ~state
              in
              Ok client_to_repeater_hook
        in
        begin match res with
        | Ok _ -> ()
        | Error e ->
          on_error ~client_name ~server_name ~state `repeater_to_server (Disconnect e);
        end;
        res
    in
    let (_ :  (Client_name.t, To_server_msg.t) Server_msg.t Stream.t) =
      Server.listen' t.server ~create_repeater_hook:(Some create_repeater_hook)
    in
    ()
  ;;

  let close_connection_from_client t client_name =
    Server.close t.server client_name;
    Option.iter (Hashtbl.find t.clients client_name) ~f:(fun client ->
      Client.close_connection client);
    Hashtbl.remove t.clients client_name;
  ;;

  let send_to_server_from t client_name msg =
    Option.iter (Hashtbl.find t.clients client_name)
      ~f:(fun client -> Client.send_ignore_errors client msg)
  ;;

  let send_from_all_clients t msg =
    Hashtbl.iter_vals t.clients ~f:(fun client -> Client.send_ignore_errors client msg)
  ;;

  let send_to_all_clients t msg =
    Server.send_to_all_ignore_errors t.server msg
  ;;

  let shutdown t =
    Server.shutdown t.server >>| fun () ->
    Hashtbl.iter_vals t.clients ~f:Client.close_connection
  ;;
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
                     && test_version <= Version.add low_version 4)

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
