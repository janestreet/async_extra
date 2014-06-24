

(* The bin-prot representation of lengths of strings, arrays, etc. *)
module Nat0 = struct
  type t = Bin_prot.Nat0.t
  (* fails on negative values *)
  let of_int_exn = Bin_prot.Nat0.of_int
  let bin_read_t = Bin_prot.Read.bin_read_nat0
  let bin_write_t = Bin_prot.Write.bin_write_nat0
  let bin_size_t = Bin_prot.Size.bin_size_nat0
end

module Protocol = struct
  (* WARNING: do not change any of these types without good reason *)

  open Bin_prot.Std
  open Sexplib.Std

  module Rpc_tag : Core.Std.Identifiable = Core.Std.String

  module Query_id = Core.Std.Unique_id.Int63(struct end)

  module Unused_query_id : sig
    type t with bin_io
    val t : t
  end = struct
    type t = Query_id.t with bin_io
    let t = Query_id.create ()
  end

  module Rpc_error = struct
    type t =
    | Bin_io_exn of Core.Std.Sexp.t
    | Connection_closed
    | Write_error of Core.Std.Sexp.t
    | Uncaught_exn of Core.Std.Sexp.t
    | Unimplemented_rpc of Rpc_tag.t * [`Version of int]
    | Unknown_query_id of Query_id.t
    with sexp, bin_io
  end

  module Rpc_result = struct
    type 'a t = ('a, Rpc_error.t) Core.Std.Result.t with bin_io
  end

  module Header = struct
    type t = int list with sexp, bin_io
  end

  module Query = struct
    type 'a needs_length = {
      tag     : Rpc_tag.t;
      version : int;
      id      : Query_id.t;
      data    : 'a;
    } with bin_io
    type 'a t = 'a needs_length with bin_read
  end

  module Response = struct
    type 'a needs_length =
      { id   : Query_id.t
      ; data : 'a Rpc_result.t
      } with bin_io
    type 'a t = 'a needs_length with bin_read
  end

  module Stream_query = struct
    type 'a needs_length = [`Query of 'a | `Abort ] with bin_io
    type 'a t = 'a needs_length with bin_read
    type nat0_t = Nat0.t needs_length with bin_read, bin_write
  end

  module Stream_initial_message = struct
    type ('response, 'error) t =
      { unused_query_id : Unused_query_id.t
      ; initial : ('response, 'error) Core.Std.Result.t
      } with bin_io
  end

  module Stream_response_data = struct
    type 'a needs_length = [`Ok of 'a | `Eof] with bin_io
    type 'a t = 'a needs_length with bin_read
    type nat0_t = Nat0.t needs_length with bin_read, bin_write
  end

  module Message = struct
    type 'a needs_length =
    | Heartbeat
    | Query of 'a Query.needs_length
    | Response of 'a Response.needs_length
    with bin_io
    type 'a t = 'a needs_length with bin_read
    type nat0_t = Nat0.t needs_length with bin_read, bin_write
  end
end

open Core.Std
open Import
open Rpc_intf

(* A hack to make the bin-prot representation of things look as though they are
   packed into bigstrings, for backwards compatibility.

   The original version of the protocol used bigstrings to hide away user-defined types.
   The bin-prot representation of a record with such a bigstring in it looks like

   {field1|field2|...|{length|content}}

   where the content is the bin-prot representation of the value being written, and the
   length is that content's length.  This writer writes data in the same format without
   using an intermediate bigstring by first calculating the size of the content, then
   writing that as a Nat0 before writing the actual content.

   When reading, we use the fact that the length can be interpreted as a bin-prot encoded
   Nat0.  The grouping is changed from

   {field1|field2|...|{length|content}}

   (only one bin-prot read) to

   {field1|field2|...|length}|{content}

   (two bin-prot reads).

   If the last field is a variant type, there will be a variant tag before the length, but
   the exact same change of grouping will work.

   Note that this only works when the bigstrings are in the last field of a record (or
   a variant in the last field of a record).  It's easy to verify that this is true.
*)
module Writer_with_length = struct
  let of_writer { Bin_prot.Type_class. write; size } =
    let write buf ~pos a =
      let len = Nat0.of_int_exn (size a) in
      let pos = Nat0.bin_write_t buf ~pos len in
      write buf ~pos a
    in
    let size a =
      let len = Nat0.of_int_exn (size a) in
      Nat0.bin_size_t len + (len :> int)
    in
    { Bin_prot.Type_class. write; size }
  ;;

  let of_type_class bin_a = of_writer bin_a.Bin_prot.Type_class.writer
end

TEST_MODULE = struct
  let bigstring_bin_prot s =
    let bigstring = Bin_prot.Utils.bin_dump String.bin_writer_t s in
    Bin_prot.Utils.bin_dump Bigstring.bin_writer_t bigstring
  ;;

  let bin_prot_with_length s =
    let writer_with_length = Writer_with_length.of_writer String.bin_writer_t in
    Bin_prot.Utils.bin_dump writer_with_length s
  ;;

  let test len =
    let s = String.create len in
    let bigstring_version = bigstring_bin_prot s in
    let with_length_version = bin_prot_with_length s in
    if Bigstring.to_string bigstring_version <> Bigstring.to_string with_length_version
    then failwithf "mismatch for length %d" len ()
  ;;

  TEST_UNIT =
    for len = 0 to Int.pow 2 10 do test len done;
    for pow = 10 to 20 do
      let x = Int.pow 2 pow in
      test (x - 1);
      test x;
      test (x + 1);
    done;
  ;;
end

(* The Result monad is also used. *)
let (>>=~) = Result.(>>=)
let (>>|~) = Result.(>>|)

(* Commute Result and Deferred. *)
let defer_result : 'a 'b. ('a Deferred.t,'b) Result.t -> ('a,'b) Result.t Deferred.t =
  function
  | Error _ as err -> return err
  | Ok d -> d >>| fun x -> Ok x

open Protocol

module Rpc_error = struct
  include Rpc_error
  include Sexpable.To_stringable (Rpc_error)

  exception Rpc of t with sexp
  let raise t = raise (Rpc t)
end

module Rpc_result : sig
  type 'a t = 'a Rpc_result.t

  type 'a try_with = location:string -> (unit -> 'a) -> 'a

  val try_with : 'a t Deferred.t try_with
  val try_with_bin_io : 'a t try_with
  val or_error : 'a t -> 'a Or_error.t
end = struct
  type 'a t = ('a, Rpc_error.t) Result.t with bin_io
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
    (fun e -> Rpc_error.Uncaught_exn e)
    ~location
    f

  (* bin_io conversions don't return deferreds *)
  let try_with_bin_io ~location f = make_try_with
    Result.try_with
    (fun x f -> f x)
    (fun e -> Rpc_error.Bin_io_exn e)
    ~location
    f

  let or_error = function
    | Ok x -> Ok x
    | Error e -> Or_error.error "rpc" e Rpc_error.sexp_of_t
end

(* utility function for bin-io'ing out of a Bigstring.t *)
let of_bigstring bin_reader_t ?add_len buf ~pos_ref ~(len : Nat0.t) ~location =
  Rpc_result.try_with_bin_io ~location (fun () ->
    let init_pos = !pos_ref in
    let data = bin_reader_t.Bin_prot.Type_class.read buf ~pos_ref in
    let add_len =
      match add_len with
      | None -> 0
      | Some add_len -> add_len data
    in
    if !pos_ref - init_pos + add_len <> (len :> int) then
      failwithf "message length (%d) did not match expected length (%d)"
        (!pos_ref - init_pos) (len : Nat0.t :> int) ();
    Ok data)

module Header : sig
  type t with bin_type_class
  val v1 : t
  val negotiate_version : t -> t -> int option
end = struct
  include Header

  let v1 = [ 1 ]

  let negotiate_version t1 t2 =
    Set.max_elt (Set.inter (Int.Set.of_list t1) (Int.Set.of_list t2))
end

module Implementation = struct
  module F = struct
    type 'connection_state t =
    | Rpc
      : 'query Bin_prot.Type_class.reader
      * 'response Bin_prot.Type_class.writer
      * ('connection_state -> 'query -> 'response Deferred.t)
      -> 'connection_state t
    | Pipe_rpc
      : 'query Bin_prot.Type_class.reader
      (* 'init can be an error or an initial state *)
      * 'init Bin_prot.Type_class.writer
      * 'update Bin_prot.Type_class.writer
      * ('connection_state
        -> 'query
        -> aborted:unit Deferred.t
        -> ('init * 'update Pipe.Reader.t, 'init) Result.t Deferred.t
        )
      -> 'connection_state t
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
             } with compare, sexp
  end

  let description t = {Description.name = Rpc_tag.to_string t.tag; version = t.version }

  let lift t ~f =
    { t with f =
        match t.f with
        | F.Rpc (bin_query, bin_response, impl) ->
          F.Rpc (bin_query, bin_response, fun state str -> impl (f state) str)
        | F.Pipe_rpc (bin_q, bin_i, bin_u, impl) ->
          F.Pipe_rpc
            (bin_q, bin_i, bin_u, fun state str ~aborted -> impl (f state) str ~aborted)
    }
end

module Implementations : sig
  type 'a t

  val create
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[ `Raise
                      | `Ignore
                      | `Call of (rpc_tag:string -> version:int -> unit)
                      ]
    -> ( 'connection_state t
       , [`Duplicate_implementations of Implementation.Description.t list]
       ) Result.t

  val null : unit -> 'a t

  val query_handler
    :  'a t
    -> (unit
        -> connection_state:'a
        -> query:Nat0.t Query.t
        -> writer:Writer.t
        -> aborted:unit Deferred.t
        -> read_buffer:Bigstring.t
        -> read_buffer_pos_ref:int ref
        -> [ `Continue | `Stop ])

  val create_exn
    :  implementations:'connection_state Implementation.t list
    -> on_unknown_rpc:[ `Raise
                      | `Ignore
                      | `Call of (rpc_tag:string -> version:int -> unit)
                      ]
    -> 'connection_state t
end = struct
  type 'a t = {
    query_handler :
      unit
      -> connection_state:'a
      -> query:Nat0.t Query.t
      -> writer:Writer.t
      -> aborted:unit Deferred.t
      -> read_buffer:Bigstring.t
      -> read_buffer_pos_ref:int ref
      -> [ `Continue | `Stop ]
    }

  let apply_implementation
      implementation
      ~connection_state
      ~query
      ~read_buffer
      ~read_buffer_pos_ref
      ~open_streaming_responses
      ~writer
      ~aborted =
    let write_response bin_writer_data data =
      let bin_writer =
        Message.bin_writer_needs_length (Writer_with_length.of_writer bin_writer_data)
      in
      Writer.write_bin_prot writer bin_writer
        (Message.Response { Response.id = query.Query.id; data; })
    in
    match implementation with
    | Implementation.F.Rpc (bin_query_reader, bin_response_writer, f) ->
      let query =
        of_bigstring bin_query_reader read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.Query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let data =
        Rpc_result.try_with ~location:"server-side rpc computation" (fun () ->
          defer_result (query >>|~ f connection_state))
      in
      data >>> write_response bin_response_writer
    | Implementation.F.Pipe_rpc
        (bin_query_reader, bin_init_writer, bin_update_writer, f) ->
      let stream_query =
        of_bigstring Stream_query.bin_reader_nat0_t
          read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.Query.data
          ~location:"server-side pipe_rpc stream_query un-bin-io'ing"
          ~add_len:(function `Abort -> 0 | `Query (len : Nat0.t) -> (len :> int))
      in
      match stream_query with
      | Error _err -> ()
      | Ok `Abort ->
        Option.iter (Hashtbl.find open_streaming_responses query.Query.id)
          ~f:(fun i -> Ivar.fill_if_empty i ());
      | Ok (`Query len) ->
        let data =
          of_bigstring bin_query_reader read_buffer ~pos_ref:read_buffer_pos_ref ~len
            ~location:"streaming_rpc server-side query un-bin-io'ing"
        in
        let user_aborted = Ivar.create () in
        Hashtbl.replace open_streaming_responses ~key:query.Query.id ~data:user_aborted;
        let aborted = Deferred.any [
          Ivar.read user_aborted;
          aborted;
        ]
        in
        let data = Rpc_result.try_with (fun () -> defer_result (
          data >>|~ fun data ->
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
          write_response bin_init_writer (Error err)
        | Ok (Error err) ->
          remove_streaming_response ();
          write_response bin_init_writer (Ok err)
        | Ok (Ok (initial, pipe_r)) ->
          write_response bin_init_writer (Ok initial);
          let bin_update_writer =
            Stream_response_data.bin_writer_needs_length
              (Writer_with_length.of_writer bin_update_writer)
          in
          don't_wait_for
            (Writer.transfer writer pipe_r (fun x ->
              write_response bin_update_writer (Ok (`Ok x))));
          Pipe.closed pipe_r >>> fun () ->
          Pipe.upstream_flushed pipe_r
          >>> function
          | `Ok | `Reader_closed ->
            write_response bin_update_writer (Ok `Eof);
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
      let shared_handler ~connection_state ~query ~writer ~aborted
          ~read_buffer ~read_buffer_pos_ref ~open_streaming_responses () =
        match Hashtbl.find implementations (query.Query.tag, query.Query.version) with
        | None ->
          let error = Rpc_error.Unimplemented_rpc
            (query.Query.tag, `Version query.Query.version)
          in
          Writer.write_bin_prot writer Message.bin_writer_nat0_t
            (Message.Response {
              Response.
              id = query.Query.id;
              data = Error error;
            });
          begin
            match on_unknown_rpc with
            | `Ignore -> ()
            | `Raise -> Rpc_error.raise error
            | `Call f -> f
              ~rpc_tag:(Rpc_tag.to_string query.Query.tag)
              ~version:query.Query.version;
          end;
          `Stop
        | Some implementation ->
          apply_implementation implementation ~connection_state ~query
            ~read_buffer ~read_buffer_pos_ref ~writer ~open_streaming_responses ~aborted;
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
    | Timeout
    | Negotiation_failed
    | Negotiated_unexpected_version of int with sexp
  end
  include T
  include Sexpable.To_stringable (T)

  exception Handshake_error of t with sexp

  let result_to_exn_result = function
    | Ok x -> Ok x
    | Error e -> Error (Handshake_error e)
end

module type Connection = Connection with module Implementations := Implementations

(* Internally, we use a couple of extra functions on connections that aren't exposed to
   users. *)
module type Connection_internal = sig
  include Connection

  module Response_handler : sig
    type t

    val create :
      (Nat0.t Response.t
       -> read_buffer:Bigstring.t
       -> read_buffer_pos_ref:int ref
       -> [ `keep | `wait of unit Deferred.t | `remove of unit Rpc_result.t ])
      -> t
  end

  val dispatch
    :  t
    -> response_handler:Response_handler.t option
    -> bin_writer_query:'a Bin_prot.Type_class.writer
    -> query:'a Query.t
    -> (unit, [`Closed]) Result.t
end

module Connection : Connection_internal = struct
  module Implementations = Implementations

  module Response_handler : sig
    type t with sexp_of

    val create :
      (Nat0.t Response.t
       -> read_buffer:Bigstring.t
       -> read_buffer_pos_ref:int ref
       -> [ `keep | `wait of unit Deferred.t | `remove of unit Rpc_result.t ])
      -> t

    val update
      :  t
      -> Nat0.t Response.t
      -> read_buffer:Bigstring.t
      -> read_buffer_pos_ref:int ref
      -> [ `already_removed
         | `keep
         | `wait of unit Deferred.t
         | `remove of unit Rpc_result.t
         ]
  end = struct
    type t =
      { f :
        (Nat0.t Response.t
         -> read_buffer:Bigstring.t
         -> read_buffer_pos_ref:int ref
         -> [`keep | `wait of unit Deferred.t | `remove of unit Rpc_result.t]) sexp_opaque
      ; mutable has_been_removed : bool
      } with sexp_of

    let create f =
      { f; has_been_removed = false }

    let update t response ~read_buffer ~read_buffer_pos_ref =
      if t.has_been_removed
      then `already_removed
      else begin
        let decision = t.f response ~read_buffer ~read_buffer_pos_ref in
        begin match decision with
        | `remove _ -> t.has_been_removed <- true
        | `wait _
        | `keep -> ()
        end;
        (decision :> [ `already_removed
                     | `keep
                     | `wait of unit Deferred.t
                     | `remove of unit Rpc_result.t ])
      end
  end

  type t =
    { reader                   : Reader.t;
      writer                   : Writer.t;
      open_queries             : (Query_id.t, Response_handler.t) Hashtbl.t;
      (* [open_streaming_responses] is only used to fill the
         [aborted] ivar on the server side *)
      open_streaming_responses : (Query_id.t, unit Ivar.t) Hashtbl.t;
      handle_query             :
           (query:Nat0.t Query.t
            -> writer:Writer.t
            -> aborted:unit Deferred.t
            -> read_buffer:Bigstring.t
            -> read_buffer_pos_ref:int ref
            -> [ `Continue | `Stop ]);
      close_started  : unit Ivar.t;
      close_finished : unit Ivar.t;
    }
  with sexp_of

  let is_closed t = Ivar.is_full t.close_started

  let writer t = if is_closed t then Error `Closed else Ok t.writer

  let bytes_to_write t = Writer.bytes_to_write t.writer

  let dispatch t ~response_handler ~bin_writer_query ~query =
    match writer t with
    | Error `Closed -> Error `Closed
    | Ok writer ->
      Writer.write_bin_prot writer
        (Message.bin_writer_needs_length (Writer_with_length.of_writer bin_writer_query))
        (Message.Query query);
      Option.iter response_handler ~f:(fun response_handler ->
        Hashtbl.replace t.open_queries ~key:query.Query.id ~data:response_handler);
      Ok ()

  let handle_query t ~query ~read_buffer ~read_buffer_pos_ref =
    t.handle_query
      ~query
      ~writer:t.writer
      ~aborted:(Ivar.read t.close_started)
      ~read_buffer
      ~read_buffer_pos_ref

  let handle_response t response ~read_buffer ~read_buffer_pos_ref =
    let unknown_query_id () =
      `Stop (Error (Rpc_error.Unknown_query_id response.Response.id))
    in
    match Hashtbl.find t.open_queries response.Response.id with
    | None -> unknown_query_id ()
    | Some response_handler ->
      match
        Response_handler.update response_handler response ~read_buffer
          ~read_buffer_pos_ref
      with
      | `keep -> `Continue
      | `wait wait -> `Wait wait
      | `already_removed -> unknown_query_id ()
      | `remove removal_circumstances ->
        Hashtbl.remove t.open_queries response.Response.id;
        begin match removal_circumstances with
        | Ok () -> `Continue
        | Error e -> `Stop (Error e)
        end

  let handle_msg t msg ~read_buffer ~read_buffer_pos_ref =
    match msg with
    | Message.Heartbeat -> `Continue
    | Message.Response response ->
      handle_response t response ~read_buffer ~read_buffer_pos_ref
    | Message.Query query ->
      match handle_query t ~query ~read_buffer ~read_buffer_pos_ref with
      | `Continue -> `Continue
      (* This [`Stop] does indicate an error, but [handle_query] handled it already. *)
      | `Stop -> `Stop (Ok ())

  let close_finished t = Ivar.read t.close_finished

  let close t =
    if not (is_closed t) then begin
      Ivar.fill t.close_started ();
      Writer.close t.writer
      >>> fun () ->
      Reader.close t.reader
      >>> fun () ->
      Ivar.fill t.close_finished ();
    end;
    close_finished t;
  ;;

  (* unfortunately, copied from reader0.ml *)
  let default_max_len = 100 * 1024 * 1024

  let create
        ?implementations
        ~connection_state
        ?max_message_size:max_len
        ?handshake_timeout
        reader
        writer
    =
    let handshake_timeout =
      Option.value
        handshake_timeout
        ~default:(Time.Span.of_sec 30.)
    in
    let implementations =
      match implementations with None -> Implementations.null () | Some s -> s
    in
    let t =
      {
        reader;
        writer;
        open_queries             = Hashtbl.Poly.create ~size:10 ();
        open_streaming_responses = Hashtbl.Poly.create ~size:10 ();
        handle_query = Implementations.query_handler implementations ~connection_state ();
        close_started  = Ivar.create ();
        close_finished = Ivar.create ();
      }
    in
    let result =
      Writer.write_bin_prot t.writer Header.bin_t.Bin_prot.Type_class.writer Header.v1;
      Deferred.choose
        [ Deferred.choice
            (* If we use [max_connections] in the server, then this read may just hang
               until the server starts accepting new connections (which could be never).
               That is why a timeout is used *)
            (Reader.read_bin_prot ?max_len t.reader
               Header.bin_t.Bin_prot.Type_class.reader)
            (fun r -> (r :> [ `Eof | `Ok of Header.t | `Timeout ]))
        ; Deferred.choice
            (Clock.after handshake_timeout)
            (fun () -> `Timeout)
        ]
      >>| fun header ->
      match header with
      | `Timeout ->
        (* There's a pending read, the reader is basically useless now, so we clean it
           up. *)
        don't_wait_for (close t);
        Error Handshake_error.Timeout
      | `Eof ->
        Error Handshake_error.Eof
      | `Ok header ->
        match Header.negotiate_version header Header.v1 with
        | None -> Error Handshake_error.Negotiation_failed
        | Some 1 ->
          let last_heartbeat = ref (Time.now ()) in
          let heartbeat () =
            if not (is_closed t) then begin
              if Time.diff (Time.now ()) !last_heartbeat > sec 30. then begin
                don't_wait_for (close t);
                Rpc_error.raise Rpc_error.Connection_closed
              end;
              Writer.write_bin_prot t.writer Message.bin_writer_nat0_t Message.Heartbeat
            end
          in
          let max_len = Option.value max_len ~default:default_max_len in
          let handle_chunk buf ~pos:init_pos ~len =
            let end_pos = init_pos + len in
            let pos_ref = ref init_pos in
            let rec loop () =
              if end_pos - !pos_ref < 8 then
                return (`Consumed (!pos_ref - init_pos, `Need 8))
              else
                let header_start = !pos_ref in
                let msg_length = Bin_prot.Read.bin_read_int_64bit buf ~pos_ref in
                if msg_length > max_len then
                  failwithf "max read length exceeded: %d > %d" msg_length max_len ();
                if end_pos - !pos_ref < msg_length then
                  (* We don't consume the header so we can read it again the next time
                     [handle_chunk] is called. *)
                  return (`Consumed (header_start - init_pos, `Need (8 + msg_length)))
                else
                  let end_of_msg = !pos_ref + msg_length in
                  let nat0_msg = Message.bin_read_nat0_t buf ~pos_ref in
                  last_heartbeat := Time.now ();
                  match
                    handle_msg t nat0_msg ~read_buffer:buf ~read_buffer_pos_ref:pos_ref
                  with
                  | `Continue ->
                    (* assume everything we needed to be read has been read or will be
                       discarded (e.g. there was a query for an unknown RPC) *)
                    pos_ref := end_of_msg;
                    loop ()
                  | `Wait wait ->
                    pos_ref := end_of_msg;
                    wait
                    >>= loop
                  | `Stop result ->
                    don't_wait_for (close t);
                    match result with
                    | Ok () -> return (`Stop_consumed ((), !pos_ref - init_pos))
                    | Error e -> Rpc_error.raise e
            in
            loop ()
          in
          begin
            let monitor = Monitor.create ~name:"RPC connection loop" () in
            Stream.iter
              (Stream.interleave (Stream.of_list (
                [ (Monitor.detach_and_get_error_stream monitor)
                ; (Monitor.detach_and_get_error_stream (Writer.monitor t.writer))
                ])))
              ~f:(fun exn ->
                don't_wait_for (close t);
                let error = match exn with
                  | Rpc_error.Rpc error -> error
                  | exn -> Rpc_error.Uncaught_exn (Exn.sexp_of_t exn)
                in
                (* clean up open streaming responses *)
                (* an unfortunate hack; ok because the response handler will have nothing
                   to read following a response where [data] is an error *)
                let dummy_buffer = Bigstring.create 1 in
                let dummy_ref = ref 0 in
                Hashtbl.iter t.open_queries
                  ~f:(fun ~key:query_id ~data:response_handler ->
                    ignore (
                      Response_handler.update
                        response_handler
                        ~read_buffer:dummy_buffer
                        ~read_buffer_pos_ref:dummy_ref
                        { Response.
                          id = query_id;
                          data = Result.Error error
                        }));
                Bigstring.unsafe_destroy dummy_buffer;
                Rpc_error.raise error);
            Scheduler.within ~monitor (fun () ->
              every ~stop:(Ivar.read t.close_started) (sec 10.) heartbeat;
              Reader.read_one_chunk_at_a_time t.reader ~handle_chunk
              >>> function
              (* The protocol is such that right now, the only outcome of the other
                 side closing the connection normally is that we get an eof. *)
              | `Eof | `Eof_with_unconsumed_data (_ : string) -> don't_wait_for (close t)
              | `Stopped () -> ())
          end;
          Ok t
        | Some i -> Error (Handshake_error.Negotiated_unexpected_version i)
    in
    result >>| Handshake_error.result_to_exn_result

  let with_close
        ?implementations
        ?max_message_size
        ?handshake_timeout
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
    create ?implementations ?max_message_size ?handshake_timeout ~connection_state
      reader writer
    >>= fun t ->
    match t with
    | Error e -> handle_handshake_error e
    | Ok t ->
      Monitor.protect ~finally:(fun () -> close t) (fun () ->
        dispatch_queries t
        >>= fun result ->
        (match implementations with
         | None -> Deferred.unit
         | Some _ -> Ivar.read t.close_finished)
        >>| fun () ->
        result
      )

  let server_with_close ?max_message_size ?handshake_timeout reader writer ~implementations
        ~connection_state ~on_handshake_error =
    let on_handshake_error =
      match on_handshake_error with
      | `Call f -> `Call f
      | `Raise -> `Raise
      | `Ignore -> `Call (fun _ -> Deferred.unit)
    in
    with_close ?max_message_size ?handshake_timeout reader writer ~implementations
      ~connection_state
      ~on_handshake_error ~dispatch_queries:(fun _ -> Deferred.unit)

  let serve
      ~implementations
      ~initial_connection_state
      ~where_to_listen
      ?max_connections
      ?max_message_size
      ?handshake_timeout
      ?(auth = (fun _ -> true))
      ?(on_handshake_error = `Ignore)
      () =
    Tcp.Server.create ?max_connections where_to_listen ~on_handler_error:`Ignore
      (fun inet r w ->
        if not (auth inet) then Deferred.unit
        else begin
          let connection_state = initial_connection_state inet in
          create ?max_message_size ?handshake_timeout ~implementations ~connection_state
            r w
          >>= function
          | Ok t -> Reader.close_finished r >>= fun () -> close t
          | Error handshake_error ->
            begin match on_handshake_error with
            | `Call f -> f handshake_error
            | `Raise  -> raise handshake_error
            | `Ignore -> ()
            end;
            Deferred.unit
        end)

  module Client_implementations = struct
    type 's t = {
      connection_state : 's;
      implementations : 's Implementations.t;
    }

    let null () = {
      connection_state = ();
      implementations = Implementations.null ();
    }
  end

  let client ~host ~port ?implementations ?max_message_size ?handshake_timeout () =
    Monitor.try_with (fun () ->
      Tcp.connect (Tcp.to_host_and_port host port)
      >>= fun (_, r, w) ->
      begin
        match implementations with
        | None ->
          let {Client_implementations.connection_state; implementations} =
            Client_implementations.null ()
          in
          create r w ?max_message_size ?handshake_timeout ~implementations
            ~connection_state
        | Some {Client_implementations.connection_state; implementations} ->
          create r w ?max_message_size ?handshake_timeout ~implementations
            ~connection_state
      end
      >>= function
      | Ok t -> Deferred.return t
      | Error handshake_error ->
        Reader.close r
        >>= fun () ->
        Writer.close w
        >>= fun () ->
        raise handshake_error)

  let with_client ~host ~port ?implementations ?max_message_size ?handshake_timeout f =
    Monitor.try_with (fun () ->
      client ~host ~port ?implementations ?max_message_size ?handshake_timeout ()
      >>= fun res ->
      begin match res with
      | Error e -> return (Error e)
      | Ok t ->
        f t     >>= fun v  ->
        close t >>= fun () ->
        return (Ok v)
      end)
    >>= fun res ->
    return (Result.join res)
end

module Rpc_common = struct
  let dispatch_raw conn ~tag ~version ~bin_writer_query ~query ~query_id ~f =
    let response_ivar = Ivar.create () in
    let query =
      { Query.
        tag
      ; version
      ; id = query_id
      ; data = query
      }
    in
    begin
      let response_handler = Some (f response_ivar) in
      match Connection.dispatch conn ~bin_writer_query ~query ~response_handler with
      | Ok () -> ()
      | Error `Closed -> Ivar.fill response_ivar (Error Rpc_error.Connection_closed)
    end;
    Ivar.read response_ivar >>| Rpc_result.or_error
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

  let bin_query t = t.bin_query
  let bin_response t = t.bin_response

  let implement t f =
    { Implementation.
      tag = t.tag;
      version = t.version;
      f =
        Implementation.F.Rpc
          (t.bin_query.Bin_prot.Type_class.reader,
           t.bin_response.Bin_prot.Type_class.writer,
           f);
    }

  let dispatch t conn query =
    let response_handler ivar =
      Connection.Response_handler.create
        (fun response ~read_buffer ~read_buffer_pos_ref ->
          let response =
            response.Response.data >>=~ fun len ->
              of_bigstring t.bin_response.Bin_prot.Type_class.reader
                read_buffer ~pos_ref:read_buffer_pos_ref ~len
                ~location:"client-side rpc response un-bin-io'ing"
          in
          Ivar.fill ivar response;
          `remove (Ok ()))
    in
    let query_id = Query_id.create () in
    Rpc_common.dispatch_raw conn ~tag:t.tag ~version:t.version
      ~bin_writer_query:t.bin_query.Bin_prot.Type_class.writer ~query ~query_id
      ~f:response_handler

  let dispatch_exn t conn query = dispatch t conn query >>| Or_error.ok_exn

end

(* the basis of the implementations of Pipe_rpc and State_rpc *)
module Streaming_rpc = struct
  module Initial_message = Stream_initial_message

  type ('query, 'initial_response, 'update_response, 'error_response) t =
    { tag : Rpc_tag.t
    ; version : int
    ; bin_query            : 'query            Bin_prot.Type_class.t
    ; bin_initial_response : 'initial_response Bin_prot.Type_class.t
    ; bin_update_response  : 'update_response  Bin_prot.Type_class.t
    ; bin_error_response   : 'error_response   Bin_prot.Type_class.t
    ; client_pushes_back   : bool
    }

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_initial_response
        ~bin_update_response ~bin_error () =
    let client_pushes_back =
      match client_pushes_back with
      | None -> false
      | Some () -> true
    in
    { tag = Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_initial_response
    ; bin_update_response
    ; bin_error_response = bin_error
    ; client_pushes_back
    }

  let implement t f =
    let f c query ~aborted =
      f c query ~aborted
      >>| fun result ->
      let init x =
        { Initial_message.
          unused_query_id = Unused_query_id.t
        ; initial = x
        }
      in
      match result with
      | Error err -> Error (init (Error err))
      | Ok (initial, pipe) -> Ok (init (Ok initial), pipe)
    in
    let bin_init_writer =
      Initial_message.bin_writer_t
        t.bin_initial_response.Bin_prot.Type_class.writer
        t.bin_error_response.Bin_prot.Type_class.writer
    in
    { Implementation.
      tag = t.tag;
      version = t.version;
      f =
        Implementation.F.Pipe_rpc
          (t.bin_query.Bin_prot.Type_class.reader,
           bin_init_writer,
           t.bin_update_response.Bin_prot.Type_class.writer,
           f);
    }

  let abort t conn id =
    let query =
      { Query.
        tag = t.tag;
        version = t.version;
        id;
        data = `Abort;
      }
    in
    ignore (
      Connection.dispatch conn ~bin_writer_query:Stream_query.bin_writer_nat0_t ~query
        ~response_handler:None : (unit,[`Closed]) Result.t
    )

  module Response_state = struct
    module State = struct
      type 'a t =
      | Waiting_for_initial_response
      | Writing_updates_to_pipe of 'a Pipe.Writer.t
    end

    type 'a t =
      { mutable state : 'a State.t }
  end

  let dispatch t conn query =
    let bin_writer_query =
      Stream_query.bin_writer_needs_length (Writer_with_length.of_type_class t.bin_query)
    in
    let query = `Query query in
    let query_id = Query_id.create () in
    let server_closed_pipe = ref false in
    let open Response_state in
    let state =
      { state = State.Waiting_for_initial_response }
    in
    let response_handler ivar =
      Connection.Response_handler.create
        (fun response ~read_buffer ~read_buffer_pos_ref ->
          match state.state with
          | State.Writing_updates_to_pipe pipe_w ->
            begin match response.Response.data with
            | Error err ->
              Pipe.close pipe_w;
              `remove (Error err)
            | Ok len ->
              let data =
                of_bigstring Stream_response_data.bin_reader_nat0_t
                  read_buffer ~pos_ref:read_buffer_pos_ref ~len
                  ~location:"client-side streaming_rpc response un-bin-io'ing"
                  ~add_len:(function `Eof -> 0 | `Ok (len : Nat0.t) -> (len :> int))
              in
              match data with
              | Error err ->
                server_closed_pipe := true;
                Pipe.close pipe_w;
                `remove (Error err)
              | Ok `Eof ->
                server_closed_pipe := true;
                Pipe.close pipe_w;
                `remove (Ok ())
              | Ok (`Ok len) ->
                let data =
                  of_bigstring t.bin_update_response.Bin_prot.Type_class.reader
                    read_buffer ~pos_ref:read_buffer_pos_ref ~len
                    ~location:"client-side streaming_rpc response un-bin-io'ing"
                in
                match data with
                | Error err ->
                  Pipe.close pipe_w;
                  `remove (Error err)
                | Ok data ->
                  if not (Pipe.is_closed pipe_w) then begin
                    Pipe.write_without_pushback pipe_w data;
                    if t.client_pushes_back
                      && (Pipe.length pipe_w) = (Pipe.size_budget pipe_w) then
                      `wait (Pipe.downstream_flushed pipe_w
                             >>| function
                             | `Ok
                             | `Reader_closed -> ())
                    else
                      `keep
                  end else
                    `keep
            end
          | State.Waiting_for_initial_response ->
            begin match response.Response.data with
            | Error err ->
              Ivar.fill ivar (Error err);
              `remove (Error err)
            | Ok len ->
              let initial = of_bigstring
                (Initial_message.bin_reader_t
                  t.bin_initial_response.Bin_prot.Type_class.reader
                  t.bin_error_response.Bin_prot.Type_class.reader)
                read_buffer ~pos_ref:read_buffer_pos_ref ~len
                ~location:"client-side streaming_rpc initial_response un-bin-io'ing"
              in
              begin match initial with
              | Error err ->
                `remove (Error err)
              | Ok initial_msg ->
                begin match initial_msg.Initial_message.initial with
                | Error err ->
                  Ivar.fill ivar (Ok (Error err));
                  `remove (Ok ())
                | Ok initial ->
                  let pipe_r, pipe_w = Pipe.create () in
                  (* Set a small buffer to reduce the number of pushback events *)
                  Pipe.set_size_budget pipe_w 100;
                  Ivar.fill ivar (Ok (Ok (query_id, initial, pipe_r)));
                  (Pipe.closed pipe_r >>> fun () ->
                   if not !server_closed_pipe then abort t conn query_id);
                  Connection.close_finished conn >>> (fun () ->
                    server_closed_pipe := true;
                    Pipe.close pipe_w);
                  state.state <- State.Writing_updates_to_pipe pipe_w;
                  `keep
                end
              end
            end)
    in
    Rpc_common.dispatch_raw conn ~query_id ~tag:t.tag ~version:t.version
      ~bin_writer_query ~query ~f:response_handler
end

(* A Pipe_rpc is like a Streaming_rpc, except we don't care about initial state - thus
   it is restricted to unit and ultimately ignored *)
module Pipe_rpc = struct
  type ('query, 'response, 'error) t = ('query, unit, 'response, 'error) Streaming_rpc.t

  module Id = struct
    type t = Query_id.t
  end

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_response ~bin_error () =
    Streaming_rpc.create
      ?client_pushes_back
      ~name ~version ~bin_query
      ~bin_initial_response: Unit.bin_t
      ~bin_update_response:  bin_response
      ~bin_error
      ()

  let bin_query t = t.Streaming_rpc.bin_query
  let bin_response t = t.Streaming_rpc.bin_update_response
  let bin_error t = t.Streaming_rpc.bin_error_response

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
    | Error rpc_error -> raise (Error.to_exn rpc_error)
    | Ok (Error _) -> raise Pipe_rpc_failed
    | Ok (Ok pipe_and_id) -> pipe_and_id

  let abort = Streaming_rpc.abort

  let name t = Rpc_tag.to_string t.Streaming_rpc.tag
  let version t = t.Streaming_rpc.version
end

module State_rpc = struct
  type ('query, 'initial, 'response, 'error) t =
    ('query, 'initial, 'response, 'error) Streaming_rpc.t

  module Id = struct
    type t = Query_id.t
  end

  let create ?client_pushes_back ~name ~version ~bin_query ~bin_state ~bin_update
        ~bin_error () =
    Streaming_rpc.create
      ?client_pushes_back
      ~name ~version ~bin_query
      ~bin_initial_response:bin_state
      ~bin_update_response:bin_update
      ~bin_error
      ()

  let bin_query t = t.Streaming_rpc.bin_query
  let bin_state t = t.Streaming_rpc.bin_initial_response
  let bin_update t = t.Streaming_rpc.bin_update_response
  let bin_error t = t.Streaming_rpc.bin_error_response

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
  let version t = t.Streaming_rpc.version
end
