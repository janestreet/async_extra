
open Core.Std
open Import

(* The Result monad is also used. *)
let (>>=~) = Result.(>>=)
let (>>|~) = Result.(>>|)

(* Commute Result and Deferred. *)
let defer_result : 'a 'b. ('a Deferred.t,'b) Result.t -> ('a,'b) Result.t Deferred.t =
  function
    | Error _ as err -> return err
    | Ok d -> d >>| fun x -> Ok x

module Rpc_tag : Identifier = String

module Query_id = Unique_id.Int63(struct end)

module Error = struct
  module T = struct
    type t =
    | Bin_io_exn of Sexp.t
    | Connection_closed
    | Write_error of Sexp.t
    | Uncaught_exn of Sexp.t
    | Unimplemented_rpc of Rpc_tag.t * [`Version of int]
    | Unknown_query_id of Query_id.t
    with sexp, bin_io
  end
  include T
  include Sexpable.To_stringable (T)

  exception Rpc of t with sexp
  let raise t = raise (Rpc t)
end

module Rpc_result : sig
  type 'a t = ('a, Error.t) Result.t
  include Binable.S1 with type 'a t := 'a t

  type 'a try_with = location:string -> (unit -> 'a) -> 'a

  val try_with : 'a t Deferred.t try_with

  val try_with_bin_io : 'a t try_with

  val to_exn_result : 'a t -> ('a, Exn.t) Result.t
end = struct
  type 'a t = ('a, Error.t) Result.t with bin_io
  type 'a try_with = location:string -> (unit -> 'a) -> 'a

  type located_error = {
    location : string;
    exn : Exn.t;
  } with sexp_of

  let make_try_with try_with (>>|) constructor ~location f =
    try_with f >>| function
      | Ok x -> x
      | Error exn -> Error (constructor (sexp_of_located_error {location; exn}))

  let try_with ~location f = make_try_with
    (Monitor.try_with ?name:None)
    (>>|)
    (fun e -> Error.Uncaught_exn e)
    ~location
    f

  (* bin_io conversions don't return deferreds *)
  let try_with_bin_io ~location f = make_try_with
    Result.try_with
    (fun x f -> f x)
    (fun e -> Error.Bin_io_exn e)
    ~location
    f

  let to_exn_result = function
    | Ok x -> Ok x
    | Error e -> Error (Error.Rpc e)
end

(* utility functions for bin-io'ing to and from a Bigstring.t *)
let to_bigstring bin_t x = Bin_prot.Utils.bin_dump
  bin_t.Bin_prot.Type_class.writer x
let of_bigstring bin_t s = Rpc_result.try_with_bin_io (fun () -> Ok (
  bin_t.Bin_prot.Type_class.reader.Bin_prot.Type_class.read s ~pos_ref:(ref 0)
))

(* For convenience since unit is a very common type for rpc arguments and return
   values. *)
module Export : sig
  val bin_unit : unit Bin_prot.Type_class.t
end = struct
  type unit_ = unit with bin_io
  let bin_unit = bin_unit_
end
include Export

module Header : sig
  type t with bin_io, sexp
  val v1 : t
  val negotiate_version : t -> t -> int option
end = struct
  type t = int list with bin_io, sexp

  let v1 = [ 1 ]

  let negotiate_version t1 t2 =
    Set.max_elt (Set.inter (Int.Set.of_list t1) (Int.Set.of_list t2))
end

module Query = struct
  type t = {
    tag     : Rpc_tag.t;
    version : int;
    id      : Query_id.t;
    data    : Bigstring.t;
  } with bin_io
end

module Response = struct
  type t = {
    id   : Query_id.t;
    data : Bigstring.t Rpc_result.t;
  } with bin_io
end

module Stream_query = struct
  type t = [`Query of Bigstring.t | `Abort ] with bin_io
end

module Stream_response_data = struct
  type t = [`Ok of Bigstring.t | `Eof] with bin_io
end

module Message = struct
  type t =
  | Heartbeat
  | Query of Query.t
  | Response of Response.t
  with bin_io
end

module Implementation = struct
  module F = struct
    type 'connection_state t =
    | Rpc of ('connection_state -> Bigstring.t -> Bigstring.t Deferred.t Rpc_result.t)
    | Pipe_rpc of (
      'connection_state
      -> Bigstring.t
      -> aborted:unit Deferred.t
      (* in the Ok case, the first Bigstring.t is the initial value to write over the wire
         and the pipe reader will get the future values to send *)
      -> (Bigstring.t * Bigstring.t Pipe.Reader.t,
          Bigstring.t) Result.t Deferred.t Rpc_result.t
    )
  end
  type 'connection_state t =
    {
      tag     : Rpc_tag.t;
      version : int;
      f       : 'connection_state F.t;
    }

  module Description = struct
    type t = { name : string;
               version : int;
             } with sexp
  end

  let description t = {Description.name = Rpc_tag.to_string t.tag; version = t.version }

end

module Server : sig
  type 'a t

  val create :
    implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[
    | `Raise
    | `Ignore
    | `Call of (rpc_tag:string -> version:int -> unit)
    ]
    -> ( 'connection_state t
       , [`Duplicate_implementations of Implementation.Description.t list]
       ) Result.t

  (* a server that can handle no queries *)
  val null : unit -> 'a t

  val query_handler :
    'a t
    -> (unit
        -> connection_state:'a
        -> query:Query.t
        -> writer:Writer.t
        -> write_response:(Writer.t -> Response.t -> unit)
        -> aborted:unit Deferred.t
        -> [ `Continue | `Stop ])

  val create_exn :
    implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[
    | `Raise
    | `Ignore
    | `Call of (rpc_tag:string -> version:int -> unit)
    ]
    -> 'connection_state t
end = struct
  type 'a t = {
    query_handler :
      unit
      -> connection_state:'a
      -> query:Query.t
      -> writer:Writer.t
      -> write_response:(Writer.t -> Response.t -> unit)
      -> aborted:unit Deferred.t
      -> [ `Continue | `Stop ]
    }

  let apply_implementation
      implementation
      ~connection_state
      ~query
      ~open_streaming_responses
      ~writer
      ~write_response
      ~aborted =
    let write_response data =
      write_response writer { Response.id = query.Query.id; data; }
    in
    match implementation with
    | Implementation.F.Rpc f ->
      let data =
        Rpc_result.try_with ~location:"server-side rpc computation" (fun () ->
          defer_result (f connection_state query.Query.data))
      in
      data >>> write_response
    | Implementation.F.Pipe_rpc f ->
      let stream_query = of_bigstring Stream_query.bin_t query.Query.data
        ~location:"server-side pipe_rpc stream_query un-bin-io'ing"
      in
      match stream_query with
      | Error _err -> ()
      | Ok `Abort ->
        Option.iter (Hashtbl.find open_streaming_responses query.Query.id)
          ~f:(fun i -> Ivar.fill_if_empty i ());
      | Ok (`Query data) ->
        let user_aborted = Ivar.create () in
        Hashtbl.replace open_streaming_responses ~key:query.Query.id ~data:user_aborted;
        let aborted = Deferred.choose_ident [
          Ivar.read user_aborted;
          aborted;
        ]
        in
        let data = Rpc_result.try_with (fun () -> defer_result (
          f connection_state data ~aborted
        )) ~location:"server-side pipe_rpc computation"
        in
        data >>> fun data ->
        let remove_streaming_response () =
          Hashtbl.remove open_streaming_responses query.Query.id
        in
        match data with
        | Error err ->
          remove_streaming_response ();
          write_response (Error err)
        | Ok (Error err) ->
          remove_streaming_response ();
          write_response (Ok err)
        | Ok (Ok (initial, pipe_r)) ->
          write_response (Ok initial);
          Writer.attach_pipe ~close_on_eof:false writer pipe_r (fun x ->
            write_response
              (Ok (to_bigstring Stream_response_data.bin_t (`Ok x))));
          Pipe.closed pipe_r >>> fun () ->
          Pipe.flushed pipe_r
          >>> function
          | `Ok | `Reader_closed ->
            write_response (Ok (to_bigstring Stream_response_data.bin_t `Eof));
            remove_streaming_response ()

  let create ~implementations:i's ~on_unknown_rpc =
    let module I = Implementation in
    (* Make sure the tags are unique. *)
    let _, dups = List.fold i's ~init:(Set.Poly.empty, Set.Poly.empty) ~f:(fun (s, dups) i ->
      let tag =
        { Implementation.Description.name = Rpc_tag.to_string i.I.tag;
          version = i.I.version;
        }
      in
      let dups = if Set.mem s tag then Set.add dups tag else dups in
      let s = Set.add s tag in
      (s, dups))
    in
    if not (Set.is_empty dups)
    then Error (`Duplicate_implementations (Set.to_list dups))
    else
      let implementations =
        Hashtbl.Poly.of_alist_exn (List.map i's ~f:(fun i ->
          ((i.I.tag, i.I.version), i.I.f))
        )
      in
      let shared_handler ~connection_state ~query ~writer ~write_response
          ~aborted ~open_streaming_responses () =
        match Hashtbl.find implementations (query.Query.tag, query.Query.version) with
        | None ->
          let error = Error.Unimplemented_rpc
            (query.Query.tag, `Version query.Query.version)
          in
          write_response writer {
            Response.
            id = query.Query.id;
            data = Error error;
          };
          begin
            match on_unknown_rpc with
            | `Ignore -> ()
            | `Raise -> Error.raise error
            | `Call f -> f
              ~rpc_tag:(Rpc_tag.to_string query.Query.tag)
              ~version:query.Query.version;
          end;
          `Stop
        | Some implementation ->
          apply_implementation implementation ~connection_state ~query
            ~writer ~write_response ~open_streaming_responses ~aborted;
          `Continue
      in
      let query_handler () =
        shared_handler ~open_streaming_responses:(Hashtbl.Poly.create ~size:10 ()) ()
      in
      Ok { query_handler }

  exception Duplicate_implementations of Implementation.Description.t list with sexp

  let create_exn ~implementations ~on_unknown_rpc =
    match create ~implementations ~on_unknown_rpc with
    | Ok x -> x
    | Error `Duplicate_implementations dups -> raise (Duplicate_implementations dups)

  let null () = create_exn ~implementations:[] ~on_unknown_rpc:`Raise

  let query_handler t = t.query_handler
end

module Handshake_error = struct
  module T = struct
    type t =
    | Eof
    | Negotiation_failed
    | Negotiated_unexpected_version of int with sexp
  end
  include T
  include Sexpable.To_stringable (T)

  exception Handshake_error of t with sexp
  let raise t = raise (Handshake_error t)
  let result_to_exn_result = function
    | Ok x -> Ok x
    | Error e -> Error (Handshake_error e)
end

(* The connection interface exposed to users in the mli. *)
module type Connection = sig
  type t

  val create :
    ?server:'s Server.t
    -> connection_state:'s
    -> ?max_message_size:int
    -> Reader.t
    -> Writer.t
    -> (t, Exn.t) Result.t Deferred.t

  val close : t -> unit
  val closed : t -> unit Deferred.t
  val already_closed : t -> bool
  val bytes_to_write : t -> int

  val with_close :
    ?server:'s Server.t
    -> connection_state:'s
    -> Reader.t
    -> Writer.t
    -> dispatch_queries:(t -> 'a Deferred.t)
    -> on_handshake_error:[
    | `Raise
    | `Call of (Exn.t -> 'a Deferred.t)
    ]
    -> 'a Deferred.t

  val server_with_close :
    Reader.t
    -> Writer.t
    -> server:'s Server.t
    -> connection_state:'s
    -> on_handshake_error:[
    | `Raise
    | `Ignore
    | `Call of (Exn.t -> unit Deferred.t)
    ]
    -> unit Deferred.t

  val serve :
    server:'s Server.t
    -> initial_connection_state:(Socket.inet -> 's)
    -> port:int
    -> ?auth:(Socket.inet -> bool)
    -> ?on_handshake_error:[
    | `Raise
    | `Ignore
    | `Call of (Exn.t -> unit)
    ]
    -> unit
    -> unit Deferred.t

  val client :
    host:string
    -> port:int
    -> (t, Exn.t) Result.t Deferred.t

  val with_client :
    host:string
    -> port:int
    -> (t -> 'a Deferred.t)
    -> ('a, Exn.t) Result.t Deferred.t
end

(* Internally, we use a couple of extra functions on connections that aren't exposed to
   users. *)
module type Connection_internal = sig
  include Connection

  type response_handler =
    Response.t
    -> [ `Keep
      | `Remove
      | `Replace of response_handler
      | `Die of Error.t] Deferred.t

  val dispatch :
    ?response_handler:response_handler
    -> t
    -> query:Query.t
    -> (unit, [`Closed]) Result.t
end

module Connection : Connection_internal = struct
  type response_handler =
    Response.t
    -> [ `Keep
      | `Remove
      | `Replace of response_handler
      | `Die of Error.t] Deferred.t

  type t =
    {
      reader                   : Reader.t;
      writer                   : Writer.t;
      open_queries             : (Query_id.t, response_handler) Hashtbl.t;
      (* [open_streaming_responses] is only used to fill the
         [aborted] ivar on the server side *)
      open_streaming_responses : (Query_id.t, unit Ivar.t) Hashtbl.t;
      handle_query             :
           (query:Query.t
            -> writer:Writer.t
            -> write_response:(Writer.t -> Response.t -> unit)
            -> aborted:unit Deferred.t
            -> [ `Continue | `Stop ]);
      eof                      : unit Ivar.t;
    }

  let writer t = if Ivar.is_full t.eof then Error `Closed else Ok t.writer
  let bytes_to_write t = Writer.bytes_to_write t.writer

  let dispatch ?response_handler t ~query =
    match writer t with
    | Error `Closed -> Error `Closed
    | Ok writer ->
      Writer.write_bin_prot writer Message.bin_writer_t (Message.Query query);
      Option.iter response_handler ~f:(fun response_handler ->
        Hashtbl.replace t.open_queries ~key:query.Query.id ~data:response_handler
      );
      Ok ()

  let handle_query t ~query =
    t.handle_query
      ~query
      ~writer:t.writer
      ~write_response:(fun writer response ->
        Writer.write_bin_prot writer Message.bin_writer_t (Message.Response response))
      ~aborted:(Ivar.read t.eof)

  let handle_response t response =
    match Hashtbl.find t.open_queries response.Response.id with
    | None ->
      Deferred.return (`Stop (Error (Error.Unknown_query_id response.Response.id)))
    | Some f ->
      f response >>| fun action ->
      begin
        match action with
        | `Remove ->
          Hashtbl.remove t.open_queries response.Response.id;
          `Continue
        | `Keep ->
          `Continue
        | `Replace f ->
          Hashtbl.replace t.open_queries ~key:response.Response.id ~data:f;
          `Continue
        | `Die err ->
          `Stop (Error err)
      end

  let handle_msg t msg =
    match msg with
    | Message.Heartbeat  -> return `Continue
    | Message.Response response -> handle_response t response
    | Message.Query query -> Deferred.return (
      match handle_query t ~query with
      | `Continue -> `Continue
      (* This [`Stop] does indicate an error, but [handle_query] handled it already. *)
      | `Stop -> `Stop Result.ok_unit
    )

  let close t =
    Ivar.fill_if_empty t.eof ();
    Deferred.whenever (Writer.close t.writer >>= fun () -> Reader.close t.reader)

  let closed t = Ivar.read t.eof
  let already_closed t = Ivar.is_full t.eof

  let create ?server ~connection_state ?max_message_size:max_len reader writer =
    let server = match server with None -> Server.null () | Some s -> s in
    let t =
      {
        reader;
        writer;
        open_queries             = Hashtbl.Poly.create ~size:10 ();
        open_streaming_responses = Hashtbl.Poly.create ~size:10 ();
        handle_query             = Server.query_handler server ~connection_state ();
        eof                      = Ivar.create ();
      }
    in
    let result =
      Writer.write_bin_prot t.writer Header.bin_writer_t Header.v1;
      Reader.read_bin_prot ?max_len t.reader Header.bin_reader_t
      >>| fun header ->
      match header with
      | `Eof ->
        Error Handshake_error.Eof
      | `Ok header ->
        match Header.negotiate_version header Header.v1 with
        | None -> Error Handshake_error.Negotiation_failed
        | Some 1 ->
          let read_message () =
            Reader.read_bin_prot ?max_len t.reader Message.bin_reader_t
          in
          let last_heartbeat = ref (Time.now ()) in
          let heartbeat () =
            if not (already_closed t) then begin
              if Time.diff (Time.now ()) !last_heartbeat > sec 30. then begin
                close t;
                Error.raise Error.Connection_closed
              end;
              Writer.write_bin_prot t.writer Message.bin_writer_t Message.Heartbeat
            end
          in
          let rec loop current_read =
            Deferred.enabled [
              Deferred.choice (Ivar.read t.eof) (fun () -> `EOF);
              Deferred.choice current_read      (fun v  -> `Read v);
            ] >>> fun filled ->
            match List.hd_exn (filled ()) with
            | `EOF -> ()
            | `Read `Eof ->
              (* The protocol is such that right now, the only outcome of the other
                 side closing the connection normally is that we get an eof. *)
              close t
            | `Read `Ok msg ->
              last_heartbeat := Time.now ();
              handle_msg t msg >>> function
              | `Continue -> loop (read_message ())
              | `Stop result ->
                close t;
                match result with
                | Ok () -> ()
                | Error e -> Error.raise e
          in
          begin
            let monitor = Monitor.create ~name:"RPC connection loop" () in
            Stream.iter
              (Stream.interleave (Stream.of_list (
                [ (Monitor.errors monitor)
                ; (Monitor.errors (Writer.monitor t.writer))
                ])))
              ~f:(fun exn ->
                Ivar.fill_if_empty t.eof ();
                let error = match exn with
                  | Error.Rpc error -> error
                  | exn -> Error.Uncaught_exn (Exn.sexp_of_t exn)
                in
                (* clean up open streaming responses *)
                Hashtbl.iter t.open_queries
                  ~f:(fun ~key:query_id ~data:response_handler ->
                    whenever (Deferred.ignore (
                      response_handler
                        { Response.
                          id = query_id;
                          data = Result.Error error
                        })));
              Error.raise error);
          Scheduler.within ~monitor (fun () ->
               Clock.every ~stop:(Ivar.read t.eof) (sec 10.) heartbeat;
               loop (read_message ()))
             end;
          Ok t
          | Some i -> Error (Handshake_error.Negotiated_unexpected_version i)
          in
        result >>| Handshake_error.result_to_exn_result

  let with_close
      ?server
      ~connection_state
      reader
      writer
      ~dispatch_queries
      ~on_handshake_error =
    let handle_handshake_error =
      match on_handshake_error with
      | `Call f -> f
      | `Raise -> raise
    in
    create ?server ~connection_state reader writer >>= fun t ->
    match t with
    | Error e -> handle_handshake_error e
    | Ok t ->
      Monitor.protect ~finally:(fun () -> close t; Deferred.unit) (fun () ->
        dispatch_queries t >>= fun result ->
        (match server with
        | None -> Deferred.unit
        | Some _ -> closed t)
        >>| fun () ->
        result
      )

  let server_with_close reader writer ~server ~connection_state ~on_handshake_error =
    let on_handshake_error =
      match on_handshake_error with
      | `Call f -> `Call f
      | `Raise -> `Raise
      | `Ignore -> `Call (fun _ -> Deferred.unit)
    in
    with_close reader writer ~server ~connection_state ~on_handshake_error
      ~dispatch_queries:(fun _ -> Deferred.unit)

  let serve
      ~server
      ~initial_connection_state
      ~port
      ?(auth = (fun _ -> true))
      ?(on_handshake_error = `Ignore)
      () =
    Tcp.serve ~port ~on_handler_error:`Ignore (fun inet r w ->
      if not (auth inet) then Deferred.unit
      else begin
        let connection_state = initial_connection_state inet in
        create ~server ~connection_state r w >>= function
        | Ok t -> Reader.closed r >>| (fun () -> close t)
        | Error handshake_error ->
          begin match on_handshake_error with
          | `Call f -> f handshake_error
          | `Raise  -> raise handshake_error
          | `Ignore -> ()
          end;
          Deferred.unit
      end)

  let with_client ~host ~port f =
    Monitor.try_with (fun () ->
      Tcp.with_connection ~host ~port (fun r w ->
        let server = Server.null () in
        create ~server ~connection_state:() r w >>= function
        | Ok t -> f t >>| (fun v -> close t; v)
        | Error handshake_error -> raise handshake_error))

  let client ~host ~port =
    Monitor.try_with (fun () ->
      Tcp.connect ~host ~port ()
      >>= fun (r,w) ->
      let server = Server.null () in
      create ~server ~connection_state:() r w >>| function
      | Ok t -> t
      | Error handshake_error -> raise handshake_error)
end

module Rpc_common = struct
  let dispatch_raw conn ~tag ~version ~query_data ~query_id ~f =
    let response_ivar = Ivar.create () in
    let query =
      { Query.
        tag
      ; version
      ; id = query_id
      ; data = query_data
      }
    in
    begin
      match Connection.dispatch conn ~query ~response_handler:(f response_ivar) with
      | Ok () -> ()
      | Error `Closed -> Ivar.fill response_ivar (Error Error.Connection_closed)
    end;
    Ivar.read response_ivar >>| Rpc_result.to_exn_result
end

module Rpc = struct
  type ('query,'response) t =
    { tag : Rpc_tag.t;
      version : int;
      bin_query    : 'query    Bin_prot.Type_class.t;
      bin_response : 'response Bin_prot.Type_class.t;
    }

  let create ~name ~version ~bin_query ~bin_response =
    { tag = Rpc_tag.of_string name;
      version;
      bin_query;
      bin_response;
    }

  let name t = Rpc_tag.to_string t.tag
  let version t = t.version

  let implement t f =
    let f c query_bigstring =
      let query = of_bigstring t.bin_query query_bigstring
        ~location:"server-side rpc query un-bin-io'ing"
      in
      query >>|~ fun query ->
        f c query >>| fun response ->
        to_bigstring t.bin_response response
    in
    { Implementation.
      tag = t.tag;
      version = t.version;
      f = Implementation.F.Rpc f;
    }

  let dispatch t conn query =
    let response_handler ivar response =
      let response =
        response.Response.data >>=~ fun data ->
          of_bigstring t.bin_response data
            ~location:"client-side rpc response un-bin-io'ing"
      in
      Ivar.fill ivar response;
      return `Remove
    in
    let query_data = to_bigstring t.bin_query query in
    let query_id = Query_id.create () in
    Rpc_common.dispatch_raw conn ~tag:t.tag ~version:t.version ~query_data ~query_id
      ~f:response_handler

  let dispatch_exn t conn query =
    dispatch t conn query
    >>| fun result ->
    match result with
    | Ok result -> result
    | Error error -> raise error

end

(* the basis of the implementations of Pipe_rpc and State_rpc *)
module Streaming_rpc = struct
  module Initial_message = struct
    type ('response, 'error) t =
      { initial : ('response, 'error) Result.t
      } with bin_io
  end

  type ('query, 'initial_response, 'update_response, 'error_response) t =
    { tag : Rpc_tag.t
    ; version : int
    ; bin_query            : 'query            Bin_prot.Type_class.t
    ; bin_initial_response : 'initial_response Bin_prot.Type_class.t
    ; bin_update_response  : 'update_response  Bin_prot.Type_class.t
    ; bin_error_response   : 'error_response   Bin_prot.Type_class.t
    }

  let create ~name ~version ~bin_query ~bin_initial_response ~bin_update_response
      ~bin_error =
    { tag = Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_initial_response
    ; bin_update_response
    ; bin_error_response = bin_error
    }

  let implement t f =
    let f c query_bigstring ~aborted =
      of_bigstring t.bin_query query_bigstring
        ~location:"streaming_rpc server-side query un-bin-io'ing"
      >>|~ fun query ->
      f c query ~aborted
      >>| fun result ->
      let init x =
        to_bigstring (Initial_message.bin_t t.bin_initial_response t.bin_error_response)
          { Initial_message.
            initial = x
          }
      in
      match result with
      | Error err -> Error (init (Error err))
      | Ok (initial, pipe) ->
        Ok (init (Ok initial),
            Pipe.map pipe ~f:(fun x -> to_bigstring t.bin_update_response x))
    in
    { Implementation.
      tag = t.tag;
      version = t.version;
      f = Implementation.F.Pipe_rpc f;
    }

  let dispatch t conn query =
    let query_data = to_bigstring Stream_query.bin_t
      (`Query (to_bigstring t.bin_query query))
    in
    let query_id = Query_id.create () in
    let response_handler ivar response =
      let read_updates pipe_w response =
        match response.Response.data with
        | Error err ->
          Pipe.close pipe_w;
          return (`Die err)
        | Ok data ->
          let data = of_bigstring Stream_response_data.bin_t data
            ~location:"client-side streaming_rpc response un-bin-io'ing"
          in
          match data with
          | Error err ->
            Pipe.close pipe_w;
            return (`Die err)
          | Ok `Eof ->
            Pipe.close pipe_w;
            return `Remove
          | Ok (`Ok data) ->
            let data = of_bigstring t.bin_update_response data
              ~location:"client-side streaming_rpc response un-bin-io'ing"
            in
            match data with
            | Error err ->
              Pipe.close pipe_w;
              return (`Die err)
            | Ok data ->
              Deferred.whenever (Pipe.write pipe_w data);
              return `Keep
      in
      let action =
        match response.Response.data with
        | Error err ->
          Ivar.fill ivar (Error err);
          `Die err
        | Ok initial_msg -> begin
          let initial = of_bigstring
            (Initial_message.bin_t t.bin_initial_response t.bin_error_response)
            initial_msg
            ~location:"client-side streaming_rpc initial_response un-bin-io'ing"
          in
          match initial with
          | Error err -> `Die err
          | Ok initial_msg -> begin
            match initial_msg.Initial_message.initial with
            | Error err ->
              Ivar.fill ivar (Ok (Error err));
              `Remove
            | Ok initial ->
              let pipe_r, pipe_w = Pipe.create () in
              Ivar.fill ivar (Ok (Ok (query_id, initial, pipe_r)));
              Connection.closed conn >>> (fun () -> Pipe.close pipe_w);
              `Replace (read_updates pipe_w)
          end
        end
      in
      return action
    in
    Rpc_common.dispatch_raw conn ~query_id ~tag:t.tag ~version:t.version ~query_data
      ~f:response_handler

  let abort t conn id =
    let query =
      { Query.
        tag = t.tag;
        version = t.version;
        id;
        data = to_bigstring Stream_query.bin_t `Abort;
      }
    in
    ignore (
      Connection.dispatch conn ~query : (unit,[`Closed]) Result.t
    )
end

(* A Pipe_rpc is like a Streaming_rpc, except we don't care about initial state - thus
   it is restricted to unit and ultimately ignored *)
module Pipe_rpc = struct
  type ('query, 'response, 'error) t = ('query, unit, 'response, 'error) Streaming_rpc.t

  module Id = struct
    type t = Query_id.t
  end

  let create ~name ~version ~bin_query ~bin_response =
    Streaming_rpc.create ~name ~version ~bin_query
      ~bin_initial_response: Unit.bin_t
      ~bin_update_response:  bin_response

  let implement t f =
    Streaming_rpc.implement t (fun a query ~aborted ->
      f a query ~aborted >>| fun x ->
      x >>|~ fun x -> (), x
    )

  let dispatch t conn query =
    Streaming_rpc.dispatch t conn query >>| fun response ->
    response >>|~ fun x ->
    x >>|~ fun (query_id, (), pipe_r) ->
    pipe_r, query_id

  exception Pipe_rpc_failed

  let dispatch_exn t conn query =
    dispatch t conn query
    >>| fun result ->
    match result with
    | Error rpc_error -> raise rpc_error
    | Ok (Error _) -> raise Pipe_rpc_failed
    | Ok (Ok pipe_and_id) -> pipe_and_id

  let abort = Streaming_rpc.abort

  let name t = Rpc_tag.to_string t.Streaming_rpc.tag
end

module State_rpc = struct
  type ('query, 'initial, 'response, 'error) t =
    ('query, 'initial, 'response, 'error) Streaming_rpc.t

  module Id = struct
    type t = Query_id.t
  end

  let create ~name ~version ~bin_query ~bin_state ~bin_update =
    Streaming_rpc.create ~name ~version ~bin_query
      ~bin_initial_response:bin_state
      ~bin_update_response:bin_update

  let implement = Streaming_rpc.implement

  let folding_map input_r ~init ~f =
    let output_r, output_w = Pipe.create () in
    let rec loop b =
      Pipe.read input_r
      >>> function
        | `Eof -> Pipe.close output_w
        | `Ok a ->
          let b = f b a in
          Pipe.write output_w b
          >>> fun () -> loop (fst b)
    in
    loop init;
    output_r

  let dispatch t conn query ~update =
    Streaming_rpc.dispatch t conn query >>| fun response ->
    response >>|~ fun x ->
    x >>|~ fun (id, state, update_r) ->
    state,
    folding_map update_r ~init:state ~f:(fun b a -> update b a, a),
    id

  let abort = Streaming_rpc.abort

  let name t = Rpc_tag.to_string t.Streaming_rpc.tag
end
