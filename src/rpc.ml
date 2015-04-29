
(* The bin-prot representation of lengths of strings, arrays, etc. *)
module Nat0 = struct
  type t = Bin_prot.Nat0.t
  (* fails on negative values *)
  let of_int_exn  = Bin_prot.Nat0.of_int
  let bin_read_t  = Bin_prot.Read.bin_read_nat0
  let bin_write_t = Bin_prot.Write.bin_write_nat0
  let bin_size_t  = Bin_prot.Size.bin_size_nat0
end

module Protocol = struct
  (* WARNING: do not change any of these types without good reason *)

  open Bin_prot.Std
  open Sexplib.Std

  module Rpc_tag : Core.Std.Identifiable = Core.Std.String

  module Query_id = Core.Std.Unique_id.Int63 ()

  module Unused_query_id : sig
    type t with bin_io
    val t : t
  end = struct
    type t = Query_id.t with bin_io
    let t = Query_id.create ()
  end

  module Rpc_error = struct
    type t =
      | Bin_io_exn        of Core.Std.Sexp.t
      | Connection_closed
      | Write_error       of Core.Std.Sexp.t
      | Uncaught_exn      of Core.Std.Sexp.t
      | Unimplemented_rpc of Rpc_tag.t * [`Version of int]
      | Unknown_query_id  of Query_id.t
    with bin_io, sexp
  end

  module Rpc_result = struct
    type 'a t = ('a, Rpc_error.t) Core.Std.Result.t with bin_io
  end

  module Header = struct
    type t = int list with bin_io, sexp
  end

  module Query = struct
    type 'a needs_length =
      { tag     : Rpc_tag.t
      ; version : int
      ; id      : Query_id.t
      ; data    : 'a
      }
    with bin_io
    type 'a t = 'a needs_length with bin_read
  end

  module Response = struct
    type 'a needs_length =
      { id   : Query_id.t
      ; data : 'a Rpc_result.t
      }
    with bin_io
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
      ; initial         : ('response, 'error) Core.Std.Result.t
      }
    with bin_io
  end

  module Stream_response_data = struct
    type 'a needs_length = [`Ok of 'a | `Eof] with bin_io
    type 'a t = 'a needs_length with bin_read
    type nat0_t = Nat0.t needs_length with bin_read, bin_write
  end

  module Message = struct
    type 'a needs_length =
      | Heartbeat
      | Query     of 'a Query.   needs_length
      | Response  of 'a Response.needs_length
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
      Nat0.bin_size_t len + ((len : Bin_prot.Nat0.t) :> int)
    in
    { Bin_prot.Type_class. write; size }
  ;;

  let of_type_class (bin_a : _ Bin_prot.Type_class.t) = of_writer bin_a.writer
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
  | Ok d ->
    match Deferred.peek d with
    | None -> d >>| fun x -> Ok x
    | Some d -> return (Ok d)

open Protocol

(* Header-writing function for some of the low-level bits that don't use the normal
   bin-prot writers. *)
let write_header_with_length =
  (* Headers can be arbitrarily long, since RPC names can be arbitrarily long, but
     50 should be long enough for most cases. *)
  let buf_ref = lazy (ref (Bigstring.create 50)) in
  fun w ~header ~followup_len ->
    if Writer.is_closed w then
      `Closed
    else begin
      let basic_header_len = Message.bin_size_nat0_t header in
      let full_header_len = basic_header_len + Bin_prot.Utils.size_header_length in
      let buf_ref = Lazy.force buf_ref in
      let orig_len = Bigstring.length !buf_ref in
      if orig_len < full_header_len then begin
        Bigstring.unsafe_destroy !buf_ref;
        buf_ref := Bigstring.create (Int.max (2 * orig_len) full_header_len);
      end;
      let buf = !buf_ref in
      let pos =
        Bin_prot.Utils.bin_write_size_header buf ~pos:0 (basic_header_len + followup_len)
      in
      let pos = Message.bin_write_nat0_t buf ~pos header in
      assert (pos = full_header_len);
      (* This copies the bytes into [w]'s buffer, so [bin_prot_buf] can be reused
         immediately. *)
      Writer.write_bigstring w buf ~pos:0 ~len:full_header_len;
      `Ok
    end

module Rpc_error = struct
  include Rpc_error
  include Sexpable.To_stringable (Rpc_error)

  exception Rpc of t with sexp
  let raise t = raise (Rpc t)
end

module Rpc_result : sig
  type 'a t = 'a Rpc_result.t

  type 'a try_with = location:string -> (unit -> 'a) -> 'a

  val uncaught_exn : location:string -> exn -> 'a t

  val try_with : ?run:[ `Now | `Schedule ] -> 'a t Deferred.t try_with

  val try_with_bin_io : 'a t try_with

  val or_error : 'a t -> 'a Or_error.t
end = struct
  type 'a t = ('a, Rpc_error.t) Result.t with bin_io
  type 'a try_with = location:string -> (unit -> 'a) -> 'a

  type located_error =
    { location : string
    ; exn : Exn.t
    }
  with sexp_of

  let uncaught_exn ~location exn =
    Error (Rpc_error.Uncaught_exn (sexp_of_located_error { location; exn }))
  ;;

  let make_try_with try_with (>>|) constructor ~location f =
    try_with f >>| function
    | Ok x -> x
    | Error exn -> Error (constructor (sexp_of_located_error { location; exn }))

  let try_with ?run ~location f =
    make_try_with
      (Monitor.try_with ?run ?name:None)
      (fun x f ->
         match Deferred.peek x with
         | None -> x >>| f
         | Some x -> return (f x))
      (fun e -> Rpc_error.Uncaught_exn e)
      ~location
      f

  (* bin_io conversions don't return deferreds *)
  let try_with_bin_io ~location f =
    make_try_with
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
let of_bigstring (bin_reader_t : _ Bin_prot.Type_class.reader)
      ?add_len buf ~pos_ref ~(len : Nat0.t) ~location =
  Rpc_result.try_with_bin_io ~location (fun () ->
    let init_pos = !pos_ref in
    let data = bin_reader_t.read buf ~pos_ref in
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

module Description = struct
  module Stable = struct
    module V1 = struct
      type t =
        { name    : string
        ; version : int
        }
      with compare, sexp, bin_io
      let hash = Hashtbl.hash
    end
  end
  include Stable.V1
  include Comparable.Make (Stable.V1)
  include Hashable.Make (Stable.V1)
end

module Implementation = struct
  module F = struct
    type 'connection_state t =
      | Blocking_rpc
        : 'query Bin_prot.Type_class.reader
          * 'response Bin_prot.Type_class.writer
          * ('connection_state -> 'query -> 'response)
        -> 'connection_state t
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

    let lift t ~f =
      match t with
      | Blocking_rpc (bin_query, bin_response, impl) ->
        Blocking_rpc (bin_query, bin_response, fun state str -> impl (f state) str)
      | Rpc (bin_query, bin_response, impl) ->
        Rpc (bin_query, bin_response, fun state str -> impl (f state) str)
      | Pipe_rpc (bin_q, bin_i, bin_u, impl) ->
        Pipe_rpc
          (bin_q, bin_i, bin_u, fun state str ~aborted -> impl (f state) str ~aborted)
  end

  type 'connection_state t =
    { tag     : Rpc_tag.t
    ; version : int
    ; f       : 'connection_state F.t
    }

  let description t = { Description. name = Rpc_tag.to_string t.tag; version = t.version }

  let lift t ~f = { t with f = F.lift ~f t.f }
end

module Implementations : sig
  type 'a t

  val create
    :  implementations : 'connection_state Implementation.t list
    -> on_unknown_rpc :
      [ `Raise
      | `Continue
      | `Close_connection
      | `Call of (rpc_tag : string -> version : int -> [ `Close_connection | `Continue ])
      ]
    -> ( 'connection_state t
       , [ `Duplicate_implementations of Description.t list ]
       ) Result.t

  val null : unit -> 'a t

  val lift : 'a t -> f:('b -> 'a) -> 'b t

  val query_handler
    :  'a t
    -> connection_state : 'a
    -> writer : Writer.t
    -> (query : Nat0.t Query.t
        -> aborted : unit Deferred.t
        -> read_buffer : Bigstring.t
        -> read_buffer_pos_ref : int ref
        -> [ `Continue | `Stop | `Wait of unit Deferred.t ]
       ) Staged.t

  val create_exn
    :  implementations : 'connection_state Implementation.t list
    -> on_unknown_rpc :
      [ `Raise
      | `Continue
      | `Close_connection
      | `Call of (rpc_tag : string -> version : int -> [ `Close_connection | `Continue ])
      ]
    -> 'connection_state t

  val add
    : 'connection_state t
    -> 'connection_state Implementation.t
    -> 'connection_state t Or_error.t

  val add_exn
    : 'connection_state t
    -> 'connection_state Implementation.t
    -> 'connection_state t

  val descriptions : _ t -> Description.t list

  module Expert : sig
    module Responder : sig
      type t
    end

    module Rpc_responder : sig
      type t = Responder.t

      val schedule
        :  t -> Bigstring.t -> pos:int -> len:int
        -> [`Connection_closed | `Flushed of unit Deferred.t]

      val write_bin_prot : t -> 'a Bin_prot.Type_class.writer -> 'a -> unit

      val write_error : t -> Error.t -> unit
    end

    val create_exn
      :  implementations : 'connection_state Implementation.t list
      -> on_unknown_rpc :
        [ `Raise
        | `Continue
        | `Close_connection
        | `Call of
            (rpc_tag : string -> version : int -> [ `Close_connection | `Continue ])
        | `Expert of
            ('connection_state -> rpc_tag : string -> version : int -> Responder.t
             -> Bigstring.t -> pos : int -> len : int -> unit Deferred.t)
        ]
      -> 'connection_state t
  end
end = struct

  module Responder = struct
    type t = Query_id.t * Writer.t
  end

  type 'connection_state on_unknown_rpc =
    [ `Raise
    | `Continue
    | `Close_connection
    | `Call of (rpc_tag : string -> version : int -> [ `Close_connection | `Continue ])
    | `Expert of
        ('connection_state
         -> rpc_tag : string
         -> version : int
         -> Responder.t
         -> Bigstring.t
         -> pos : int
         -> len : int
         -> unit Deferred.t)
    ]

  type 'connection_state t =
    { implementations : 'connection_state Implementation.F.t Description.Table.t
    ; on_unknown_rpc : 'connection_state on_unknown_rpc
    }

  let descriptions t = Hashtbl.keys t.implementations

  let apply_implementation
        implementation
        ~connection_state
        ~(query : _ Query.t)
        ~read_buffer
        ~read_buffer_pos_ref
        ~open_streaming_responses
        ~writer
        ~aborted =
    let write_response bin_writer_data data =
      let bin_writer =
        Message.bin_writer_needs_length (Writer_with_length.of_writer bin_writer_data)
      in
      Writer.write_bin_prot writer bin_writer (Response { id = query.id; data })
    in
    match implementation with
    | Implementation.F.Blocking_rpc (bin_query_reader, bin_response_writer, f) ->
      let query =
        of_bigstring bin_query_reader read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let data =
        try query >>|~ f connection_state with
        | exn ->
          Rpc_result.uncaught_exn
            ~location:"server-side blocking rpc computation"
            exn
      in
      write_response bin_response_writer data
    | Implementation.F.Rpc (bin_query_reader, bin_response_writer, f) ->
      let query =
        of_bigstring bin_query_reader read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side rpc query un-bin-io'ing"
      in
      let data =
        Rpc_result.try_with ~run:`Now
          ~location:"server-side rpc computation" (fun () ->
            defer_result (query >>|~ f connection_state))
      in
      (* In the common case that the implementation returns a value immediately, we will
         write the response immediately as well (this is also why the above [try_with] has
         [~run:`Now]).  This can be a big performance win for servers that get many
         queries in a single Async cycle. *)
      ( match Deferred.peek data with
        | None -> data >>> write_response bin_response_writer
        | Some data -> write_response bin_response_writer data )
    | Implementation.F.Pipe_rpc
        (bin_query_reader, bin_init_writer, bin_update_writer, f) ->
      let stream_query =
        of_bigstring Stream_query.bin_reader_nat0_t
          read_buffer ~pos_ref:read_buffer_pos_ref
          ~len:query.data
          ~location:"server-side pipe_rpc stream_query un-bin-io'ing"
          ~add_len:(function `Abort -> 0 | `Query (len : Nat0.t) -> (len :> int))
      in
      match stream_query with
      | Error _err -> ()
      | Ok `Abort ->
        Option.iter (Hashtbl.find open_streaming_responses query.id)
          ~f:(fun i -> Ivar.fill_if_empty i ());
      | Ok (`Query len) ->
        let data =
          of_bigstring bin_query_reader read_buffer ~pos_ref:read_buffer_pos_ref ~len
            ~location:"streaming_rpc server-side query un-bin-io'ing"
        in
        let user_aborted = Ivar.create () in
        Hashtbl.set open_streaming_responses ~key:query.id ~data:user_aborted;
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
          Hashtbl.remove open_streaming_responses query.id
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
    (* Make sure the tags are unique. *)
    let implementations = Description.Table.create ~size:10 () in
    let dups = Description.Hash_set.create ~size:10 () in
    List.iter i's ~f:(fun (i : _ Implementation.t) ->
      let description =
        { Description.
          name    = Rpc_tag.to_string i.tag
        ; version = i.version
        }
      in
      match Hashtbl.add implementations ~key:description ~data:i.f with
      | `Ok -> ()
      | `Duplicate -> Hash_set.add dups description
    );
    if not (Hash_set.is_empty dups) then
      Error (`Duplicate_implementations (Hash_set.to_list dups))
    else
      Ok { implementations; on_unknown_rpc = (on_unknown_rpc :> _ on_unknown_rpc) }


  let shared_handler implementations on_unknown_rpc
        ~open_streaming_responses
        ~connection_state ~(query : Nat0.t Query.t) ~writer ~aborted
        ~read_buffer ~read_buffer_pos_ref =
    match
      Hashtbl.find implementations
        { Description.name = Rpc_tag.to_string query.tag
        ; version = query.version}
    with
    | Some implementation ->
      apply_implementation implementation ~connection_state ~query ~read_buffer
        ~read_buffer_pos_ref ~writer ~open_streaming_responses ~aborted;
      `Continue
    | None ->
      match on_unknown_rpc with
      | `Expert impl ->
        let {Query.tag; version; id; data = len} = query in
        let d =
          impl connection_state ~rpc_tag:(Rpc_tag.to_string tag) ~version (id, writer)
            read_buffer ~pos:!read_buffer_pos_ref ~len:(len :> int)
        in
        if Deferred.is_determined d
        then `Continue
        else `Wait d
      | (`Continue | `Raise | `Close_connection | `Call _) as on_unknown_rpc ->
        let error = Rpc_error.Unimplemented_rpc (query.tag, `Version query.version) in
        Writer.write_bin_prot writer Message.bin_writer_nat0_t
          (Response
             { id   = query.id
             ; data = Error error
             });
        begin
          match on_unknown_rpc with
          | `Continue         -> `Continue
          | `Raise            -> Rpc_error.raise error
          | `Close_connection -> `Stop
          | `Call f ->
            match f ~rpc_tag:(Rpc_tag.to_string query.tag) ~version:query.version with
            | `Close_connection -> `Stop
            | `Continue         -> `Continue
        end

  let query_handler {implementations; on_unknown_rpc} ~connection_state ~writer =
    let open_streaming_responses = Hashtbl.Poly.create ~size:10 () in
    Staged.stage (
      shared_handler implementations on_unknown_rpc ~open_streaming_responses
        ~connection_state ~writer
    )

  exception Duplicate_implementations of Description.t list with sexp

  let create_exn ~implementations ~on_unknown_rpc =
    match create ~implementations ~on_unknown_rpc with
    | Ok x -> x
    | Error `Duplicate_implementations dups -> raise (Duplicate_implementations dups)

  let null () = create_exn ~implementations:[] ~on_unknown_rpc:`Raise

  let add_exn t (implementation : _ Implementation.t) =
    let desc : Description.t =
      { name = Rpc_tag.to_string implementation.tag
      ; version = implementation.version
      }
    in
    let implementations = Hashtbl.copy t.implementations in
    match Hashtbl.add implementations ~key:desc ~data:implementation.f with
    | `Duplicate -> raise (Duplicate_implementations [desc])
    | `Ok -> { t with implementations }

  let add t implementation =
    Or_error.try_with (fun () -> add_exn t implementation)


  let lift {implementations;on_unknown_rpc; _} ~f =
    let implementations =
      Hashtbl.map implementations ~f:(Implementation.F.lift ~f)
    in
    let on_unknown_rpc =
      match on_unknown_rpc with
      | `Raise | `Continue | `Close_connection | `Call _ as x -> x
      | `Expert expert -> `Expert (fun state -> expert (f state))
    in
    { implementations ; on_unknown_rpc }

  module Expert = struct
    module Responder = Responder

    module Rpc_responder = struct
      type t = Responder.t

      let schedule (id, w) buf ~pos ~len =
        let header : Nat0.t Message.t = Response {id; data = Ok (Nat0.of_int_exn len)} in
        match write_header_with_length w ~header ~followup_len:len with
        | `Closed -> `Connection_closed
        | `Ok ->
          Writer.schedule_bigstring w buf ~pos ~len;
          `Flushed (Writer.flushed w)

      let write_error (id, w) error =
        if not (Writer.is_closed w) then
          let data =
            Rpc_result.uncaught_exn ~location:"server-side raw rpc computation"
              (Error.to_exn error)
          in
          Writer.write_bin_prot w Message.bin_writer_nat0_t (Response {id; data})

      let write_bin_prot (id, w) bin_writer_a a =
        if not (Writer.is_closed w) then
          Writer.write_bin_prot w
            (Message.bin_writer_needs_length (Writer_with_length.of_writer bin_writer_a))
            (Response {id; data = Ok a})
    end

    let create_exn = create_exn
  end
end

module Handshake_error = struct
  module T = struct
    type t =
      | Eof
      | Timeout
      | Negotiation_failed
      | Negotiated_unexpected_version of int
    with sexp
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

  type response_handler
    =  Nat0.t Response.t
    -> read_buffer : Bigstring.t
    -> read_buffer_pos_ref : int ref
    -> [ `keep
       | `wait of unit Deferred.t
       | `remove of unit Rpc_result.t
       | `remove_and_wait of unit Deferred.t
       ]

  val dispatch
    :  t
    -> response_handler : response_handler option
    -> bin_writer_query : 'a Bin_prot.Type_class.writer
    -> query            : 'a Query.t
    -> (unit, [`Closed]) Result.t

  val schedule_dispatch
    :  t
    -> tag : Rpc_tag.t
    -> version : int
    -> Bigstring.t
    -> pos : int
    -> len : int
    -> response_handler : response_handler
    -> [`Connection_closed | `Flushed of unit Deferred.t]
end

module Connection : Connection_internal = struct
  type response_handler
    =  Nat0.t Response.t
    -> read_buffer : Bigstring.t
    -> read_buffer_pos_ref : int ref
    -> [ `keep
       | `wait of unit Deferred.t
       | `remove of unit Rpc_result.t
       | `remove_and_wait of unit Deferred.t
       ]

  type t =
    { reader : Reader.t
    ; writer : Writer.t
    ; open_queries : (Query_id.t, response_handler sexp_opaque) Hashtbl.t
    ; close_started  : unit Ivar.t
    ; close_finished : unit Ivar.t
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
        (Query query);
      Option.iter response_handler ~f:(fun response_handler ->
        Hashtbl.set t.open_queries ~key:query.id ~data:response_handler);
      Ok ()

  let schedule_dispatch t ~tag ~version buf ~pos ~len ~response_handler =
    let id = Query_id.create () in
    let header : Nat0.t Message.t =
      Query {tag; version; id; data = Nat0.of_int_exn len}
    in
    match write_header_with_length t.writer ~header ~followup_len:len with
    | `Closed -> `Connection_closed
    | `Ok ->
      Writer.schedule_bigstring t.writer buf ~pos ~len;
      Hashtbl.set t.open_queries ~key:id ~data:response_handler;
      `Flushed (Writer.flushed t.writer)

  let handle_response t (response : _ Response.t) ~read_buffer ~read_buffer_pos_ref =
    match Hashtbl.find t.open_queries response.id with
    | None -> `Stop (Error (Rpc_error.Unknown_query_id response.id))
    | Some response_handler ->
      match response_handler response ~read_buffer ~read_buffer_pos_ref with
      | `keep -> `Continue
      | `wait wait -> `Wait wait
      | `remove_and_wait wait ->
        Hashtbl.remove t.open_queries response.id;
        `Wait wait
      | `remove removal_circumstances ->
        Hashtbl.remove t.open_queries response.id;
        begin match removal_circumstances with
        | Ok () -> `Continue
        | Error e ->
          match e with
          | Unimplemented_rpc _ -> `Continue
          | Bin_io_exn _
          | Connection_closed
          | Write_error _
          | Uncaught_exn _
          | Unknown_query_id _ -> `Stop (Error e)
        end

  let handle_msg t (msg : _ Message.t) ~handle_query ~read_buffer ~read_buffer_pos_ref =
    match msg with
    | Heartbeat -> `Continue
    | Response response ->
      handle_response t response ~read_buffer ~read_buffer_pos_ref
    | Query query ->
      match
        handle_query ~query ~aborted:(Ivar.read t.close_started)
          ~read_buffer ~read_buffer_pos_ref
      with
      | `Continue -> `Continue
      | `Wait d -> `Wait d
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

  let default_handshake_timeout = Time.Span.of_sec 30.

  let create
        ?implementations
        ~connection_state
        ?max_message_size:max_len
        ?(handshake_timeout = default_handshake_timeout)
        reader
        writer
    =
    let implementations =
      match implementations with None -> Implementations.null () | Some s -> s
    in
    let t =
      { reader
      ; writer
      ; open_queries   = Hashtbl.Poly.create ~size:10 ()
      ; close_started  = Ivar.create ()
      ; close_finished = Ivar.create ()
      }
    in
    let result =
      Writer.write_bin_prot t.writer Header.bin_t.writer Header.v1;
      Deferred.choose
        [ Deferred.choice
            (* If we use [max_connections] in the server, then this read may just hang
               until the server starts accepting new connections (which could be never).
               That is why a timeout is used *)
            (Reader.read_bin_prot ?max_len t.reader
               Header.bin_t.reader)
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
      | `Eof -> Error Eof
      | `Ok header ->
        match Header.negotiate_version header Header.v1 with
        | None -> Error Negotiation_failed
        | Some 1 ->
          let handle_query =
            Staged.unstage (
              Implementations.query_handler implementations ~writer
                ~connection_state:(connection_state t)
            )
          in
          let last_heartbeat = ref (Time.now ()) in
          let heartbeat () =
            if not (is_closed t) then begin
              if Time.diff (Time.now ()) !last_heartbeat > sec 30. then begin
                don't_wait_for (close t);
                Rpc_error.raise Connection_closed
              end;
              Writer.write_bin_prot t.writer Message.bin_writer_nat0_t Heartbeat
            end
          in
          let max_len = Option.value max_len ~default:default_max_len in
          let handle_chunk buf ~pos:init_pos ~len =
            let end_pos = init_pos + len in
            let pos_ref = ref init_pos in
            let finish_loop ~consumed ~need ~wait_before_reading =
              let ret_val = `Consumed (consumed, `Need need) in
              match wait_before_reading with
              | [] -> return ret_val (* extremely common case *)
              | l -> Deferred.all_unit l >>| fun () -> ret_val
            in
            let rec loop wait_before_reading =
              if end_pos - !pos_ref < 8 then
                finish_loop ~consumed:(!pos_ref - init_pos) ~need:8 ~wait_before_reading
              else
                let header_start = !pos_ref in
                let msg_length = Bin_prot.Read.bin_read_int_64bit buf ~pos_ref in
                if msg_length > max_len then
                  failwithf "max read length exceeded: %d > %d" msg_length max_len ();
                if end_pos - !pos_ref < msg_length then
                  (* We don't consume the header so we can read it again the next time
                     [handle_chunk] is called. *)
                  finish_loop ~consumed:(header_start - init_pos) ~need:(8 + msg_length)
                    ~wait_before_reading
                else
                  let end_of_msg = !pos_ref + msg_length in
                  let nat0_msg = Message.bin_read_nat0_t buf ~pos_ref in
                  last_heartbeat := Time.now ();
                  match
                    handle_msg t nat0_msg ~handle_query ~read_buffer:buf
                      ~read_buffer_pos_ref:pos_ref
                  with
                  | `Continue ->
                    (* assume everything we needed to be read has been read or will be
                       discarded (e.g. there was a query for an unknown RPC) *)
                    pos_ref := end_of_msg;
                    loop wait_before_reading
                  | `Wait wait ->
                    pos_ref := end_of_msg;
                    loop (wait :: wait_before_reading)
                  | `Stop result ->
                    don't_wait_for (close t);
                    match result with
                    | Ok () -> return (`Stop_consumed ((), !pos_ref - init_pos))
                    | Error e -> Rpc_error.raise e
            in
            loop []
          in
          begin
            let cleanup ~raise_ exn =
              don't_wait_for (close t);
              let error = match exn with
                | Rpc_error.Rpc error -> error
                | exn -> Uncaught_exn (Exn.sexp_of_t exn)
              in
              (* clean up open streaming responses *)
              (* an unfortunate hack; ok because the response handler will have nothing
                 to read following a response where [data] is an error *)
              let dummy_buffer = Bigstring.create 1 in
              let dummy_ref = ref 0 in
              Hashtbl.iter t.open_queries
                ~f:(fun ~key:query_id ~data:response_handler ->
                  ignore (
                    response_handler
                      ~read_buffer:dummy_buffer
                      ~read_buffer_pos_ref:dummy_ref
                      { id   = query_id
                      ; data = Error error
                      }));
              Bigstring.unsafe_destroy dummy_buffer;
              if raise_ then Rpc_error.raise error
            in
            let monitor = Monitor.create ~name:"RPC connection loop" () in
            Stream.iter
              (Stream.interleave (Stream.of_list (
                 [ (Monitor.detach_and_get_error_stream monitor)
                 ; (Monitor.detach_and_get_error_stream (Writer.monitor t.writer))
                 ])))
              ~f:(fun exn -> cleanup ~raise_:true exn);
            Scheduler.within ~monitor (fun () ->
              every ~stop:(Ivar.read t.close_started) (sec 10.) heartbeat;
              Reader.read_one_chunk_at_a_time t.reader ~handle_chunk
              >>> function
                (* The protocol is such that right now, the only outcome of the other
                   side closing the connection normally is that we get an eof. *)
              | `Eof
              | `Eof_with_unconsumed_data (_ : string) ->
                cleanup ~raise_:false (Rpc_error.Rpc Connection_closed)
              | `Stopped () -> ()
            )
          end;
          Ok t
        | Some i -> Error (Negotiated_unexpected_version i)
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
        ?max_pending_connections
        ?buffer_age_limit
        ?max_message_size
        ?handshake_timeout
        ?(auth = (fun _ -> true))
        ?(on_handshake_error = `Ignore)
        () =
    Tcp.Server.create ?max_connections ?max_pending_connections
      ?buffer_age_limit where_to_listen
      ~on_handler_error:`Ignore
      (fun inet r w ->
         if not (auth inet) then
           Deferred.unit
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
        ?max_message_size
        ?buffer_age_limit
        ?(handshake_timeout = default_handshake_timeout)
        () =
    let finish_handshake_by = Time.add (Time.now ()) handshake_timeout in
    Monitor.try_with (fun () ->
      Tcp.connect ?buffer_age_limit ~timeout:handshake_timeout
        (Tcp.to_host_and_port ?via_local_interface host port))
    >>=? fun (_, r, w) ->
    let handshake_timeout = Time.diff finish_handshake_by (Time.now ()) in
    begin
      match implementations with
      | None ->
        let { Client_implementations. connection_state; implementations } =
          Client_implementations.null ()
        in
        create r w ?max_message_size ~handshake_timeout ~implementations
          ~connection_state
      | Some { Client_implementations. connection_state; implementations } ->
        create r w ?max_message_size ~handshake_timeout ~implementations
          ~connection_state
    end
    >>= function
    | Ok _ as ok -> return ok
    | Error _ as error ->
      Reader.close r
      >>= fun () ->
      Writer.close w
      >>= fun () ->
      return error

  let with_client ~host ~port
        ?via_local_interface
        ?implementations
        ?max_message_size
        ?handshake_timeout
        f =
    client ?via_local_interface ~host ~port
      ?implementations
      ?max_message_size
      ?handshake_timeout
      ()
    >>=? fun t ->
    try_with (fun () -> f t)
    >>= fun result ->
    close t
    >>| fun () ->
    result
end

module Rpc_common = struct
  let dispatch_raw conn ~tag ~version ~bin_writer_query ~query ~query_id ~f =
    let response_ivar = Ivar.create () in
    let query =
      { Query.
        tag
      ; version
      ; id      = query_id
      ; data    = query
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
    { tag          : Rpc_tag.t
    ; version      : int
    ; bin_query    : 'query    Bin_prot.Type_class.t
    ; bin_response : 'response Bin_prot.Type_class.t
    }

  let create ~name ~version ~bin_query ~bin_response =
    { tag          = Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_response
    }

  let name t = Rpc_tag.to_string t.tag
  let version t = t.version

  let description t =
    { Description.
      name    = name t
    ; version = version t
    }

  let bin_query t = t.bin_query
  let bin_response t = t.bin_response

  let implement t f =
    { Implementation.
      tag     = t.tag
    ; version = t.version
    ; f       = Rpc (t.bin_query.reader, t.bin_response.writer, f)
    }

  let implement' t f =
    { Implementation.
      tag     = t.tag
    ; version = t.version
    ; f       = Blocking_rpc (t.bin_query.reader, t.bin_response.writer, f)
    }

  let dispatch t conn query =
    let response_handler ivar =
      fun response ~read_buffer ~read_buffer_pos_ref ->
        let response =
          response.Response.data >>=~ fun len ->
          of_bigstring t.bin_response.reader
            read_buffer ~pos_ref:read_buffer_pos_ref ~len
            ~location:"client-side rpc response un-bin-io'ing"
        in
        Ivar.fill ivar response;
        `remove (Ok ())
    in
    let query_id = Query_id.create () in
    Rpc_common.dispatch_raw conn ~tag:t.tag ~version:t.version
      ~bin_writer_query:t.bin_query.writer ~query ~query_id
      ~f:response_handler

  let dispatch_exn t conn query = dispatch t conn query >>| Or_error.ok_exn

  TEST_UNIT "Open dispatches see connection closed error" =
    Thread_safe.block_on_async_exn (fun () ->
      let bin_t = Bin_prot.Type_class.bin_unit in
      let rpc =
        create ~version:1
          ~name:"__TEST_Async_rpc.Rpc" ~bin_query:bin_t ~bin_response:bin_t
      in
      let serve () =
        let implementation =
          implement rpc (fun () () ->
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
        let res = dispatch rpc connection () in
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

  module Expert = struct
    module Responder = Implementations.Expert.Rpc_responder

    let schedule_dispatch conn ~rpc_tag ~version buf ~pos ~len ~handle_response
          ~handle_error =
      let response_handler : Connection.response_handler =
        fun response ~read_buffer ~read_buffer_pos_ref ->
          match response.data with
          | Error e ->
            handle_error (Error.t_of_sexp (Rpc_error.sexp_of_t e));
            `remove (Ok ())
          | Ok len ->
            let len = (len : Nat0.t :> int) in
            let d = handle_response read_buffer ~pos:!read_buffer_pos_ref ~len in
            read_buffer_pos_ref := !read_buffer_pos_ref + len;
            if Deferred.is_determined d
            then `remove (Ok ())
            else `remove_and_wait d
      in
      Connection.schedule_dispatch conn ~tag:(Rpc_tag.of_string rpc_tag) ~version
        buf ~pos ~len ~response_handler
  end
end

(* the basis of the implementations of Pipe_rpc and State_rpc *)
module Streaming_rpc = struct
  module Initial_message = Stream_initial_message

  type ('query, 'initial_response, 'update_response, 'error_response) t =
    { tag                  : Rpc_tag.t
    ; version              : int
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
    { tag                  = Rpc_tag.of_string name
    ; version
    ; bin_query
    ; bin_initial_response
    ; bin_update_response
    ; bin_error_response   = bin_error
    ; client_pushes_back
    }

  let implement t f =
    let f c query ~aborted =
      f c query ~aborted
      >>| fun result ->
      let init x =
        { Initial_message.
          unused_query_id = Unused_query_id.t
        ; initial         = x
        }
      in
      match result with
      | Error err -> Error (init (Error err))
      | Ok (initial, pipe) -> Ok (init (Ok initial), pipe)
    in
    let bin_init_writer =
      Initial_message.bin_writer_t
        t.bin_initial_response.writer
        t.bin_error_response.writer
    in
    { Implementation.
      tag      = t.tag
    ; version  = t.version
    ; f        = Pipe_rpc (t.bin_query.reader,
                           bin_init_writer,
                           t.bin_update_response.writer,
                           f);
    }

  let abort t conn id =
    let query =
      { Query.
        tag     = t.tag
      ; version = t.version
      ; id
      ; data    = `Abort
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
        | Writing_updates_to_pipe      of 'a Pipe.Writer.t
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
      { state = Waiting_for_initial_response }
    in
    let response_handler ivar : Connection.response_handler =
      fun response ~read_buffer ~read_buffer_pos_ref ->
        match state.state with
        | Writing_updates_to_pipe pipe_w ->
          begin match response.data with
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
                of_bigstring t.bin_update_response.reader
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
                  && (Pipe.length pipe_w) = (Pipe.size_budget pipe_w)
                  then `wait (Pipe.downstream_flushed pipe_w
                              >>| function
                              | `Ok
                              | `Reader_closed -> ())
                  else `keep
                end else
                  `keep
          end
        | Waiting_for_initial_response ->
          begin match response.data with
          | Error err ->
            Ivar.fill ivar (Error err);
            `remove (Error err)
          | Ok len ->
            let initial =
              of_bigstring
                (Initial_message.bin_reader_t
                   t.bin_initial_response.reader
                   t.bin_error_response.reader)
                read_buffer ~pos_ref:read_buffer_pos_ref ~len
                ~location:"client-side streaming_rpc initial_response un-bin-io'ing"
            in
            begin match initial with
            | Error err ->
              `remove (Error err)
            | Ok initial_msg ->
              begin match initial_msg.initial with
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
                state.state <- Writing_updates_to_pipe pipe_w;
                `keep
              end
            end
          end
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

  let description t =
    { Description.
      name    = name t
    ; version = version t
    }
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

  let bin_query  t = t.Streaming_rpc.bin_query
  let bin_state  t = t.Streaming_rpc.bin_initial_response
  let bin_update t = t.Streaming_rpc.bin_update_response
  let bin_error  t = t.Streaming_rpc.bin_error_response

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

  let description t =
    { Description.
      name = name t
    ; version = version t
    }
end

module Any = struct
  type t =
    | Rpc   : ('q, 'r) Rpc.t               -> t
    | Pipe  : ('q, 'r, 'e) Pipe_rpc.t      -> t
    | State : ('q, 's, 'u, 'e) State_rpc.t -> t
end
