open Core.Std
open Import

module Protocol = struct
  let version = 1

  module Open_file = struct
    module Mode = struct
      type t =
        | Read
        | Tail
        with sexp, bin_io
    end

    module Error = struct
      type t =
        | File_not_found of string
        | Unknown of string
        with sexp, bin_io

      let to_string t = Sexp.to_string_hum (sexp_of_t t)
    end

    module Query = struct
      type t = Open of string * Mode.t with sexp, bin_io
    end

    module Message : sig
      type t =
        | String of string
        | Bigstring of Bigstring.t
        with sexp_of,bin_io

      val length        : t -> int
      val to_string     : t -> string option
      val to_string_exn : t -> string
      val to_bigstring  : t -> Bigstring.t
    end = struct
      type t =
        | String of string
        | Bigstring of Bigstring.t
        with bin_io

      let length t =
        match t with
        | String s     -> String.length s
        | Bigstring bs -> Bigstring.length bs

      let to_string_exn t =
        match t with
        | String s     -> s
        | Bigstring bs ->
          Bigstring.to_string bs ~pos:0 ~len:(Bigstring.length bs)

      let to_string t =
        try
          Some (to_string_exn t)
        with
        | _ -> None

      let to_bigstring t =
        match t with
        | String s     -> Bigstring.of_string s
        | Bigstring bs -> bs

      let sexp_of_t t =
        match t with
        | String s -> Sexp.List [Sexp.Atom "String"; Sexp.Atom s]
        | Bigstring _ -> Sexp.Atom "Bigstring <opaque>"
    end

    module Response = struct
      type t = (Message.t, Error.t) Result.t with bin_io
    end

    let rpc =
      Rpc.Pipe_rpc.create
        ~name:"open_file"
        ~version
        ~bin_query:Query.bin_t
        ~bin_response:Response.bin_t
        ~bin_error:Unit.bin_t
  end
end

let canonicalize filename =
  (* Remove multiple slashes in [filename].  It would be nice to use [realpath] for
     this, but I think it's problematic on occasion, since it seems to insist the
     file exists.  -- mshinwell *)
  let non_empty s = String.length s > 0 in
  let reform remainder = String.concat (List.filter remainder ~f:non_empty) ~sep:"/" in
  match String.split filename ~on:'/' with
  | ""::remainder -> "/" ^ (reform remainder)
  | remainder -> reform remainder

module Server = struct
  module File = struct

    type t = {
      filename : string;
      writer : [ `Writer of File_writer.t | `This_is_a_static_file ];
      tail     : Protocol.Open_file.Message.t Tail.t;
      line_ending : [ `Dos | `Unix ];
      mutable num_lines_on_disk_after_flushing_writer : int;
      mutable closed : bool;
    } with sexp_of
  end

  module Atomic_operations : sig
    val snapshot_state : File.t
      -> ([ `Read_this_many_lines_from_disk of int ]
            * [ `Then_read_from of Protocol.Open_file.Message.t Stream.t ])
         Deferred.t

    val write_message : File.t -> string -> File_writer.t -> unit
    val schedule_message : File.t -> Bigstring.t -> File_writer.t -> unit
  end = struct
    let snapshot_state t =
      (* begin critical section -- no Async context switch allowed *)
      let num_on_disk = t.File.num_lines_on_disk_after_flushing_writer in
      let lines_read_without_hitting_the_disk = Tail.collect t.File.tail in
      (* end critical section *)
      (match t.File.writer with
       | `Writer writer -> File_writer.flushed writer
       | `This_is_a_static_file -> (assert (Tail.is_closed t.File.tail); Deferred.unit))
      >>| fun () ->
      (* The file is divided into two sections: the first [num_on_disk] lines (read from
         disk) and a subsequent portion (read from the Tail).  The invariant is that
         these two portions form a contiguous portion of the file (i.e. they do not
         overlap and there is no gap between them). *)
      `Read_this_many_lines_from_disk num_on_disk,
        `Then_read_from lines_read_without_hitting_the_disk

    let line_ending_to_string = function
      | `Dos -> "\r\n"
      | `Unix -> "\n"

    let already_has_line_ending s ~line_ending ~get ~length =
      let line_ending_str = line_ending_to_string line_ending in
      let line_ending_length = String.length line_ending_str in
      let s_length = length s in
      if s_length < line_ending_length then
        false
      else
        let expect_line_ending_at = s_length - line_ending_length in
        List.for_all (List.range 0 line_ending_length)
          ~f:(fun x -> get s (expect_line_ending_at + x) = String.get line_ending_str x)

    let write_core t ~msg ~writer ~write_to_file ~length ~get ~protocol_msg =
      let line_ending = t.File.line_ending in
      (* begin critical section -- no Async context switch allowed *)
      t.File.num_lines_on_disk_after_flushing_writer <-
        t.File.num_lines_on_disk_after_flushing_writer + 1;
      write_to_file writer msg;
      if not (already_has_line_ending msg ~line_ending ~get ~length) then
        File_writer.write writer (line_ending_to_string line_ending);
      Tail.extend t.File.tail protocol_msg
      (* end critical section *)

    let write_message t msg writer =
      write_core t
        ~msg
        ~writer
        ~write_to_file:File_writer.write
        ~length:String.length
        ~get:String.get
        ~protocol_msg:(Protocol.Open_file.Message.String msg)

    let schedule_message t msg writer =
      write_core t
        ~msg
        ~writer
        ~write_to_file:File_writer.schedule_bigstring
        ~length:Bigstring.length
        ~get:Bigstring.get
        ~protocol_msg:(Protocol.Open_file.Message.Bigstring msg)
  end

  module State = struct
    type t = {
      files : File.t String.Table.t;
      mutable serving_on : [ `Not_yet_serving | `Port of int ];
    } with sexp_of

    let global =
      { files = String.Table.create ();
        serving_on = `Not_yet_serving;
      }
  end

  let debug_snapshot () = State.sexp_of_t State.global

  let with_aborted input ~aborted =
    Deferred.choose [
      Deferred.choice aborted (fun () -> `Aborted);
      Deferred.choice input (fun v -> `Read v);
    ]

  let send_msg_to_client w msg = Pipe.write w (Ok msg)

  let tail stream w aborted =
    Deferred.create (fun ivar ->
      let stop_tailing = Ivar.fill ivar in
      let rec loop stream =
        with_aborted (Stream.next stream) ~aborted
        >>> function
        | `Aborted | `Read Stream.Nil -> stop_tailing ()
        | `Read (Stream.Cons (msg, rest)) ->
          send_msg_to_client w msg >>> fun () -> loop rest
      in
      loop stream)

  exception Unexpected_eof_when_reading_lines of
    string * [ `Wanted_to_read of int ] * [ `But_only_managed of int ] with sexp

  let read t w aborted =
    Atomic_operations.snapshot_state t
    >>= function (`Read_this_many_lines_from_disk num_from_disk, `Then_read_from rest) ->
    Reader.with_file t.File.filename ~f:(fun r ->
      Deferred.create (fun ivar ->
        let stop_reading = Ivar.fill ivar in
        let rec read_lines ~current_line =
          if current_line <= num_from_disk then
            with_aborted (Reader.read_line r) ~aborted
            >>> function
            | `Read `Ok msg ->
              send_msg_to_client w (Protocol.Open_file.Message.String msg) >>> fun () ->
              read_lines ~current_line:(current_line + 1)
            | `Aborted      -> stop_reading ()
            | `Read `Eof    ->
              raise (Unexpected_eof_when_reading_lines (t.File.filename,
                `Wanted_to_read num_from_disk,
                `But_only_managed (current_line - 1)))
          else (
            whenever (Reader.close r);
            tail rest w aborted >>> stop_reading
          )
        in
        read_lines ~current_line:1))

  let tail t w aborted = tail (Tail.collect t.File.tail) w aborted

  let handle_open_file state query ~aborted =
    let module Open_file = Protocol.Open_file in
    let (Open_file.Query.Open (filename, mode)) = query in
    let pipe_r, pipe_w = Pipe.create () in
    aborted >>> (fun () -> Pipe.close pipe_w);
    Monitor.try_with (fun () ->
      let dispatch filename f =
        match String.Table.find state.State.files (canonicalize filename) with
        | None   ->
          Pipe.write pipe_w
            (Error (Open_file.Error.File_not_found filename));
          >>| fun () ->
          Pipe.close pipe_w
        | Some file -> f file pipe_w aborted
      in
      match mode with
      | Open_file.Mode.Read -> dispatch filename read
      | Open_file.Mode.Tail -> dispatch filename tail)
    >>> (function
    | Ok ()   -> Pipe.close pipe_w
    | Error e ->
      if not (Pipe.is_closed pipe_w) then begin
        whenever (Pipe.write pipe_w
          (Error (Open_file.Error.Unknown (Exn.to_string e))));
        Pipe.close pipe_w
      end);
    Deferred.return (Ok pipe_r)

  let implementations = [
    Rpc.Pipe_rpc.implement Protocol.Open_file.rpc handle_open_file;
  ]

  let serve ~auth ~port =
    let server =
      Rpc.Server.create ~implementations ~on_unknown_rpc:`Ignore
      |! function
      | Ok s -> State.global.State.serving_on <- `Port port; s
      | Error (`Duplicate_implementations _) -> assert false
    in
    Rpc.Connection.serve ~auth ~server ~port ()
      ~initial_connection_state:(fun _ -> State.global)

  exception File_is_already_open_in_tcp_file of string with sexp

  let count_lines filename =
    Sys.file_exists filename
    >>= function
    | `No      -> Deferred.return 0
    | `Unknown -> failwithf "unable to open file: %s" filename ()
    | `Yes ->
      (* There is no strong case for using [exclusive:true] here, since locks are
         advisory, but it expresses something in the code that we want to be true, and
         shouldn't hurt. *)
      Reader.with_file ~exclusive:true filename
        ~f:(fun r -> Pipe.drain_and_count (Reader.lines r))
  ;;

  let open_file ?(append=false) ?(dos_format = false) filename =
    let filename = canonicalize filename in
    match String.Table.find State.global.State.files filename with
    | Some _ -> raise (File_is_already_open_in_tcp_file filename)
    | None   ->
      let num_lines_already_on_disk =
        if append
        then count_lines filename
        else return 0
      in
      num_lines_already_on_disk
      >>= fun num_lines_already_on_disk ->
      File_writer.create filename ~append
      >>| fun writer ->
      let file =
        { File.
          filename;
          writer      = `Writer writer;
          tail        = Tail.create ();
          line_ending = if dos_format then `Dos else `Unix;
          num_lines_on_disk_after_flushing_writer = num_lines_already_on_disk;
          closed = false;
        }
      in
      String.Table.replace State.global.State.files ~key:filename ~data:file;
      file
  ;;

  let stop_serving_internal t =
    String.Table.remove State.global.State.files t.File.filename
  ;;

  let stop_serving = stop_serving_internal

  let close ?(stop_serving=true) t =
    if t.File.closed
    then Deferred.unit
    else begin
      t.File.closed <- true;
      if stop_serving then stop_serving_internal t;
      Tail.close_if_open t.File.tail;
      match t.File.writer with
      | `Writer writer -> File_writer.close writer
      | `This_is_a_static_file -> Deferred.unit
    end

  exception Attempt_to_flush_static_tcp_file of string with sexp

  let flushed t =
    match t.File.writer with
    | `Writer writer -> File_writer.flushed writer
    | `This_is_a_static_file -> raise (Attempt_to_flush_static_tcp_file t.File.filename)

  exception Attempt_to_write_message_to_closed_tcp_file of string with sexp
  exception Attempt_to_write_message_to_static_tcp_file of string with sexp

  let gen_message t f =
    if t.File.closed then
      raise (Attempt_to_write_message_to_closed_tcp_file t.File.filename);
    match t.File.writer with
    | `Writer writer -> f writer
    | `This_is_a_static_file ->
      raise (Attempt_to_write_message_to_static_tcp_file t.File.filename)

  let write_message t msg =
    gen_message t (fun writer -> Atomic_operations.write_message t msg writer)

  let schedule_message t msg =
    gen_message t (fun writer -> Atomic_operations.schedule_message t msg writer)

  let write_sexp =
    (* We use strings for Sexps whose string representations can fit on the minor heap and
       Bigstring.t's for those that can't. *)
    let max_num_words_allocatable_on_minor_heap = 256 in
    let bytes_per_word = 8 in
    let buf = Bigbuffer.create 1024 in
    fun t sexp ->
      Bigbuffer.clear buf;
      Sexp.to_buffer_gen sexp ~buf ~add_char:Bigbuffer.add_char
        ~add_string:Bigbuffer.add_string;
      let buf_size_in_words =
        (* This is the same calculation that the runtime uses.

           Remember that space is left in the Caml value for a NULL terminator.
           So if the word size is 8 bytes and the string is 8 bytes long, we
           need two words, for example. *)
        (Bigbuffer.length buf + bytes_per_word) / bytes_per_word
      in
      if buf_size_in_words <= max_num_words_allocatable_on_minor_heap then
        write_message t (Bigbuffer.contents buf)
      else
        schedule_message t (Bigbuffer.big_contents buf);
  ;;

  let with_file ?append filename ~f =
    open_file ?append filename >>= fun t ->
    Monitor.try_with (fun () -> f t) >>= fun res ->
    close t >>| fun () ->
    Result.ok_exn res

  let serve_existing_static_file filename =
    let filename = canonicalize filename in
    match String.Table.find State.global.State.files filename with
    | None   ->
      count_lines filename >>| fun num_lines_on_disk_after_flushing_writer ->
      let tail = Tail.create () in
      Tail.close_if_open tail;
      let file =
        { File.
          filename;
          writer = `This_is_a_static_file;
          line_ending = `Unix;  (* Arbitrary setting: will never be used. *)
          tail;
          num_lines_on_disk_after_flushing_writer;
          closed = true;
        }
      in
      String.Table.replace State.global.State.files ~key:filename ~data:file
    | Some _ -> raise (File_is_already_open_in_tcp_file filename)

  let writer_monitor t =
    match t.File.writer with
    | `Writer writer -> Ok (File_writer.monitor writer)
    | `This_is_a_static_file -> Error `This_is_a_static_file
end

module Client = struct
  type t = Rpc.Connection.t

  module File_id = struct
    type t = Rpc.Pipe_rpc.Id.t
  end

  module Error    = Protocol.Open_file.Error
  module Message  = Protocol.Open_file.Message
  module Response = Protocol.Open_file.Response

  let connect ~host ~port = Rpc.Connection.client ~host ~port
  let disconnect t = Rpc.Connection.close t

  let read t filename =
    let filename = canonicalize filename in
    Rpc.Pipe_rpc.dispatch_exn
      Protocol.Open_file.rpc
      t
      (Protocol.Open_file.Query.Open (filename, Protocol.Open_file.Mode.Read))
    >>| fun (pipe_r, id) ->
    Pipe.closed pipe_r >>> (fun () ->
      Rpc.Pipe_rpc.abort Protocol.Open_file.rpc t id);
    pipe_r

  let tail t filename =
    let filename = canonicalize filename in
    Rpc.Pipe_rpc.dispatch_exn
      Protocol.Open_file.rpc
      t
      (Protocol.Open_file.Query.Open (filename, Protocol.Open_file.Mode.Tail))
    >>| fun (pipe_r, id) ->
    Pipe.closed pipe_r >>> (fun () ->
      Rpc.Pipe_rpc.abort Protocol.Open_file.rpc t id);
    pipe_r

  let close t id = Rpc.Pipe_rpc.abort Protocol.Open_file.rpc t id
end
