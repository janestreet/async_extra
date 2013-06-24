
open Core.Std
open Import

module Level = struct
  type t = [
    | `Debug
    | `Info
    | `Error ]
    with sexp, bin_io

  let to_string = function
    | `Debug -> "Debug"
    | `Info  -> "Info"
    | `Error -> "Error"
  ;;

  let of_string = function
    | "Debug" -> `Debug
    | "Info"  -> `Info
    | "Error" -> `Error
    | s       -> failwithf "not a valid level %s" s ()

  (* Ordering of log levels in terms of verbosity. *)
  let equal_or_more_verbose_than l1 l2 =
    match l1,l2 with
    | `Error,`Error            -> true
    | `Error, (`Debug | `Info) -> false
    | `Info,  (`Info | `Error) -> true
    | `Info,`Debug             -> false
    | `Debug, _                -> true
  ;;
end

module Rotation = struct
  (* description of bounderies for file rotation.  If all fields are None the file will
     never be rotated.  Any field set to Some _ will cause rotation to happen when that
     boundary is crossed.  Multiple boundaries may be set.  Log rotation always causes
     incrementing rotation conditions (e.g. size) to reset, though this is the
     responsibililty of the caller to should_rotate.
  *)
  type t = {
    messages      : int option;
    size          : Byte_units.t option;
    time          : (Time.Ofday.t * Zone.t) option;
    keep          : [ `All | `Newer_than of Time.Span.t | `At_least of int ];
    naming_scheme : [ `Numbered | `Timestamped ]
  } with sexp,fields

  let should_rotate t ~last_messages ~last_size ~last_time =
    Fields.fold ~init:false
      ~messages:(fun acc field ->
        match Field.get field t with
        | None              -> acc
        | Some rotate_messages -> acc || rotate_messages <= last_messages)
      ~size:(fun acc field ->
        match Field.get field t with
        | None             -> acc
        | Some rotate_size -> acc || Byte_units.(<=) rotate_size last_size)
      ~time:(fun acc field ->
        match Field.get field t with
        | None -> acc
        | Some (rotation_ofday,zone) ->
          let rotation_time = Time.of_date_ofday zone (Date.today ()) rotation_ofday in
          let current_time  = Time.now () in
          acc || (current_time >= rotation_time && last_time <= rotation_time))
      ~keep:(fun acc _ -> acc)
      ~naming_scheme:(fun acc _ -> acc)
  ;;
end

module Message : sig
  type t with sexp, bin_io

  val create  : Level.t option -> tags:(string * string) list -> string Lazy.t -> t
  val time    : t -> Time.t
  val level   : t -> Level.t option
  val message : t -> string
  val tags    : t -> (string * string) list

  val to_write_only_text : t -> string

  val write_write_only_text : t -> Writer.t -> unit
  val write_sexp            : t -> Writer.t -> unit
  val write_bin_prot        : t -> Writer.t -> unit
end = struct
  module T = struct
    type 'a t = {
      time    : Time.t;
      level   : Level.t option;
      message : 'a;
      tags    : (string * string) list;
    } with sexp,bin_io
  end
  open T

  type t = string Lazy.t T.t

  let to_lazy concrete_t =
    {
      time    = concrete_t.time;
      level   = concrete_t.level;
      message = lazy concrete_t.message;
      tags    = concrete_t.tags
    }
  ;;

  let of_lazy lazy_t =
    {
      time    = lazy_t.time;
      level   = lazy_t.level;
      message = Lazy.force lazy_t.message;
      tags    = lazy_t.tags
    }
  ;;

  include Bin_prot.Utils.Make_binable (struct
    module Binable = struct
      type t = string T.t with bin_io
    end

    let to_binable = of_lazy
    let of_binable = to_lazy

    type t = string Lazy.t T.t
  end)

  let sexp_of_t t    = <:sexp_of<string t>> (of_lazy t)
  let t_of_sexp sexp = to_lazy (<:of_sexp<string t>> sexp)

  let create level ~tags message = { time = Time.now (); level; message; tags }

  let time t    = t.time
  let level t   = t.level
  let message t = Lazy.force t.message
  let tags t    = t.tags

  let to_write_only_text t =
    let prefix =
      match t.level with
      | None   -> ""
      | Some l -> Level.to_string l ^ " "
    in
    String.concat ~sep:"" [
      Time.to_string_abs t.time;
      " ";
      prefix;
      Lazy.force t.message
    ]
  ;;

  let write_write_only_text t wr =
    Writer.write wr (to_write_only_text t);
    Writer.newline wr
  ;;

  let write_sexp t wr =
    Writer.write_sexp wr (sexp_of_t t);
    Writer.newline wr
  ;;

  let write_bin_prot t wr =
    Writer.write_bin_prot wr bin_writer_t t
  ;;
end

module Output : sig
  (* The output module exposes a variant that describes the output type and sub-modules
     that each expose a write function (or create that returns a write function) that is
     of type: Level.t -> string -> unit Deferred.t.  It is the responsibility of the write
     function to contain all state, and to clean up after itself.
  *)
  type machine_readable_format = [`Sexp | `Bin_prot ]
  type format = [ machine_readable_format | `Text ]

  type t with sexp_of

  val create : (Message.t Queue.t -> unit Deferred.t) -> t
  val apply : t -> Message.t Queue.t -> unit Deferred.t

  val screen        : t
  val writer        : format -> Writer.t -> t
  val file          : format -> filename:string -> t
  val rotating_file : format -> basename:string -> Rotation.t -> t

  val combine : t list -> t
end = struct
  type machine_readable_format = [`Sexp | `Bin_prot ]
  type format = [ machine_readable_format | `Text ]

  type t = Message.t Queue.t -> unit Deferred.t

  let create = ident
  let apply t = t

  let sexp_of_t _ = Sexp.Atom "<opaque>"

  let combine writers =
    (fun msg -> Deferred.all_unit (List.map writers ~f:(fun write -> write msg)))
  ;;

  let basic_write format w msg =
    begin match format with
    | `Sexp ->
      Message.write_sexp msg w
    | `Bin_prot ->
      Message.write_bin_prot msg w
    | `Text ->
      Message.write_write_only_text msg w
    end;
  ;;

  module Screen : sig
    val write : t
  end = struct
    let write =
      (fun msgs ->
        let flush_stdout = ref false in
        let flush_stderr = ref false in
        let stdout () =
          flush_stdout := true;
          Lazy.force Writer.stdout
        in
        let stderr () =
          flush_stderr := true;
          Lazy.force Writer.stderr
        in
        Queue.iter msgs ~f:(fun m ->
          let out =
            match Message.level m with
            | None       -> stdout ()
            | Some level ->
              match level with
              | `Error         -> stderr ()
              | `Info | `Debug -> stdout ()
          in
          basic_write `Text out m);
        if !flush_stdout then Writer.flushed (stdout ()) else Deferred.unit
        >>= fun () ->
        if !flush_stderr then Writer.flushed (stderr ()) else Deferred.unit)
    ;;
  end

  module File : sig
    val write' : format -> filename:string -> (Message.t Queue.t -> Int63.t Deferred.t)
    val create : format -> filename:string -> t
  end = struct
    let write' format ~filename msgs =
      Writer.with_file ~append:true filename ~f:(fun w ->
        Queue.iter msgs ~f:(fun msg -> basic_write format w msg);
        Writer.flushed w
        >>| fun () ->
        Writer.bytes_written w)
    ;;

    let create format ~filename = (fun msgs ->
      write' format ~filename msgs >>| fun (_ : Int63.t) -> ())
    ;;
  end

  module Writer : sig
    val create : format -> Writer.t -> t
  end = struct
    (* The writer output type takes no responsibility over the Writer.t it is given.  In
       particular it makes no attempt to ever close it. *)
    let create format w = (fun msgs ->
      Queue.iter msgs ~f:(fun msg -> basic_write format w msg);
      Writer.flushed w)
  end

  module Rotating_file : sig
    val create
      :  format
      -> basename:string
      -> Rotation.t
      -> t
  end = struct
    module type Id_intf = sig
      type t
      val create : unit -> t
      val rotate_one : t -> t

      val to_string_opt : t -> string option
      val of_string_opt : string option -> t option
      val ascending : t -> t -> int
    end

    module Make (Id:Id_intf) = struct
      let make_filename ~dirname ~basename id =
        match Id.to_string_opt id with
        | None   -> dirname ^/ sprintf "%s.log" basename
        | Some s -> dirname ^/ sprintf "%s.%s.log" basename s
      ;;

      let parse_filename_id ~basename filename =
        if Filename.basename filename = basename ^ ".log" then Id.of_string_opt None
        else begin
          let open Option.Monad_infix in
          String.chop_prefix (Filename.basename filename) ~prefix:(basename ^ ".")
          >>= fun id_dot_log ->
          String.chop_suffix id_dot_log ~suffix:".log"
          >>= fun id ->
          Id.of_string_opt (Some id)
        end
      ;;

      let current_log_files ~dirname ~basename =
        Sys.readdir dirname
        >>| fun files ->
        List.filter_map (Array.to_list files) ~f:(fun filename ->
          let filename = dirname ^/ filename in
          Option.(parse_filename_id ~basename filename >>| fun id -> id, filename))
      ;;

      let maybe_delete_old_logs ~dirname ~basename keep =
        begin match keep with
        | `All -> return []
        | `Newer_than span ->
          current_log_files ~dirname ~basename
          >>= fun files ->
          let cutoff = Time.sub (Time.now ()) span in
          Deferred.List.filter files ~f:(fun (_,filename) ->
            Unix.stat filename
            >>| fun stats -> Time.(<) stats.Unix.Stats.mtime cutoff)
        | `At_least i ->
          current_log_files ~dirname ~basename
          >>| fun files ->
          List.drop (List.sort files ~cmp:(fun (i1,_) (i2,_) -> Id.ascending i1 i2)) i
        end
        >>= Deferred.List.iter ~f:(fun (_i,filename) -> Unix.unlink filename)
      ;;

      let rotate ~dirname ~basename keep =
        current_log_files ~dirname ~basename
        >>= fun files ->
        let files =
          List.rev (List.sort files ~cmp:(fun (i1,_) (i2, _) -> Id.ascending i1 i2))
        in
        Deferred.List.iter files ~f:(fun (id, src) ->
          let id' = Id.rotate_one id in
          if Id.ascending id id' <> 0
          then Unix.rename ~src ~dst:(make_filename ~dirname ~basename id')
          else Deferred.unit)
        >>= fun () ->
        maybe_delete_old_logs ~dirname ~basename keep
      ;;

      let create format ~basename rotation =
        let last_messages  = ref 0 in
        let last_size      = ref 0 in
        let filename       = ref "" in
        let last_time      = ref (Time.now ()) in
        let basename, dirname =
        (* Fix dirname, because cwd may change *)
          match Filename.is_absolute basename with
          | true  -> Filename.basename basename, return (Filename.dirname basename)
          | false -> basename, Sys.getcwd ()
        in
        let finished_rotation =
          dirname >>= fun dirname ->
          filename := (make_filename ~dirname ~basename (Id.create ()));
          rotate ~dirname ~basename rotation.Rotation.keep
          >>| const dirname
        in
        fun msgs ->
          finished_rotation >>= fun dirname ->
          begin
            if
              Rotation.should_rotate rotation ~last_messages:!last_messages
                ~last_size:(Byte_units.create `Bytes (Float.of_int !last_size))
                ~last_time:!last_time
            then begin
              rotate ~dirname ~basename rotation.Rotation.keep
              >>| fun () ->
              filename := make_filename ~dirname ~basename (Id.create ())
            end
            else Deferred.unit
          end
          >>= fun () ->
          File.write' format ~filename:(!filename) msgs
          >>| fun size ->
          last_messages := !last_messages + 1;
          last_size     := !last_size + Int63.to_int_exn size;
          last_time     := Time.now ()
      ;;
    end

    module Numbered = Make (struct
      type t            = int
      let create        = const 0
      let rotate_one    = (+) 1
      let to_string_opt = function 0 -> None | x -> Some (Int.to_string x)
      let ascending     = Int.ascending

      let of_string_opt = function
        | None   -> Some 0
        | Some s -> try Some (Int.of_string s) with _ -> None
      ;;
    end)

    module Timestamped = Make (struct
      type t               = Time.t
      let create           = Time.now
      let rotate_one       = ident
      let to_string_opt ts = Some (Time.to_filename_string ts)
      let ascending        = Time.ascending

      let of_string_opt    = function
        | None   -> None
        | Some s -> try Some (Time.of_filename_string s) with _ -> None
      ;;
    end)

    let create format ~basename rotation =
      match rotation.Rotation.naming_scheme with
      | `Numbered    -> Numbered.create format ~basename rotation
      | `Timestamped -> Timestamped.create format ~basename rotation
    ;;
  end

  let rotating_file = Rotating_file.create
  let file          = File.create
  let writer        = Writer.create
  let screen        = Screen.write
end

(* A log is a pipe that can take one of three messages.
   | Msg (level, msg) -> write the message to the current output if the level is
   appropriate
   | New_level l -> set the level of the output
   | New_output f -> set the output function for future messages to f
   | Flush i -> used to get around the current odd design of Pipe flushing.  Sends an
   ivar that the reading side fills in after it has finished handling all
   previous messages.

   The f delivered by New_output must not hold on to any resources that normal garbage
   collection won't clean up.  When New_output is delivered to the pipe the current
   write function will be discarded without notification.  If this proves to be a
   resource problem (too many syscalls for instance) then we could add an on_discard
   function to writers that we call when a new writer appears.
*)
module Update = struct
  type t =
    | Msg of Message.t
    | New_level of Level.t
    | New_output of Output.t
    | Flush of unit Ivar.t
    with sexp_of

  let to_string t = Sexp.to_string (sexp_of_t t)
end

type t = Update.t Pipe.Writer.t

let push_update t update =
  if not (Pipe.is_closed t)
  then Pipe.write_without_pushback t update
  else
    failwithf "Log: can't process %s because this log has been closed"
      (Update.to_string update) ()
;;

let flushed t = Deferred.create (fun i -> push_update t (Update.Flush i))

let closed = Pipe.is_closed

module Flush_at_exit_or_gc : sig
  val add_log : t -> unit
  val close   : t -> unit
end = struct
  module Weak_table = Caml.Weak.Make (struct
    type z = t
    type t = z
    let equal = Pipe.equal
    let hash  = Pipe.hash
  end)

  (* contains all logs we want to flush at shutdown *)
  let flush_bag = lazy (Bag.create ())

  (* contains all currently live logs. *)
  let live_logs = lazy (Weak_table.create 1)

  (* [flush] adds a flush deferred to the flush_bag *)
  let flush t =
    if not (closed t) then begin
      let flush_bag = Lazy.force flush_bag in
      let flushed   = flushed t in
      let tag       = Bag.add flush_bag flushed in
      upon flushed (fun () -> Bag.remove flush_bag tag)
    end
  ;;

  let close t =
    if not (closed t) then begin
      flush t;
      Pipe.close t
    end
  ;;

  let finish_at_shutdown =
    lazy
      (Shutdown.at_shutdown (fun () ->
        let live_logs = Lazy.force live_logs in
        let flush_bag = Lazy.force flush_bag in
        Weak_table.iter (fun log -> flush log) live_logs;
        Deferred.all_unit (Bag.to_list flush_bag)))
  ;;

  let add_log log =
    let live_logs = Lazy.force live_logs in
    Lazy.force finish_at_shutdown;
    Weak_table.remove live_logs log;
    Weak_table.add live_logs log;
    (* If we fall out of scope just close and flush normally.  Without this we risk being
       finalized and removed from the weak table before the the shutdown handler runs, but
       also before we get all of logs out of the door. *)
    Gc.add_finalizer_exn log close;
  ;;
end

let close = Flush_at_exit_or_gc.close

let create_log_processor ~level ~output =
  let write         = ref (Output.combine output) in
  let current_level = ref level in
  stage (fun updates ->
    let msgs = Queue.create () in
    let output_message_queue f =
      if Queue.length msgs = 0
      then f ()
      else begin
        let msgs' = Queue.create () in
        Queue.transfer ~src:msgs ~dst:msgs';
        (Output.apply !write) msgs'
        >>= f
      end
    in
    let rec loop () =
      match Queue.dequeue updates with
      | None        -> output_message_queue (fun () -> Deferred.unit)
      | Some update ->
        match update with
        | Update.Flush i ->
          output_message_queue (fun () ->
            Ivar.fill i ();
            loop ())
        | Update.Msg msg ->
          let enqueue =
            match Message.level msg with
            | None       -> true
            | Some level ->
              Level.equal_or_more_verbose_than !current_level level
          in
          if enqueue then Queue.enqueue msgs msg;
          loop ()
        | Update.New_level level ->
          current_level := level;
          loop ()
        | Update.New_output f ->
          output_message_queue (fun () ->
            write := f;
            loop ())
    in
    loop ())
;;

let create ~level ~output : t =
  let r,w = Pipe.create () in
  let process_log = unstage (create_log_processor ~level ~output) in
  don't_wait_for (Pipe.iter' r ~f:process_log);
  Flush_at_exit_or_gc.add_log w;
  w
;;

let set_output t outputs =
  push_update t (Update.New_output (Output.combine outputs))
;;

let set_level t level =
  push_update t (Update.New_level level)
;;

let of_lazy ?(tags=[]) ?level t msg =
  let msg = Message.create level ~tags msg in
  push_update t (Update.Msg msg)
;;

let message t msg = push_update t (Update.Msg msg)

let printf ?tags ?level t k =
  ksprintf (fun msg -> of_lazy ?tags ?level t (lazy msg)) k
;;

let sexp ?tags ?level t v to_sexp =
  of_lazy ?tags ?level t (lazy (Sexp.to_string_mach (to_sexp v)))
;;

let raw ?tags t k   = printf ?tags t k
let debug ?tags t k = printf ?tags t ~level:`Debug k
let info ?tags t k  = printf ?tags t ~level:`Info k
let error ?tags t k = printf ?tags t ~level:`Error k

module type Global_intf = sig
  val log : t Lazy.t

  val set_level  : Level.t -> unit
  val set_output : Output.t list -> unit
  val raw        : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val info       : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val error      : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val debug      : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val flushed    : unit -> unit Deferred.t

  val printf
    :  ?tags:(string * string) list
    -> ?level:Level.t
    -> ('a, unit, string, unit) format4
    -> 'a

  val sexp
    :  ?tags:(string * string) list
    -> ?level:Level.t
    -> 'a
    -> ('a -> Sexp.t)
    -> unit

  val of_lazy
    :  ?tags:(string * string) list
    -> ?level:Level.t
    -> string Lazy.t
    -> unit

  val message : Message.t -> unit
end

module Make_global(Empty : sig end) : Global_intf = struct
  let log               = lazy (create ~level:`Info ~output:[Output.screen])
  let set_level level   = set_level (Lazy.force log) level
  let set_output output = set_output (Lazy.force log) output

  let raw   ?tags k = raw ?tags (Lazy.force log) k
  let info  ?tags k = info ?tags (Lazy.force log) k
  let error ?tags k = error ?tags (Lazy.force log) k
  let debug ?tags k = debug ?tags (Lazy.force log) k
  let flushed () = flushed (Lazy.force log)
  let printf  ?tags ?level k = printf ?tags ?level (Lazy.force log) k
  let sexp    ?tags ?level v to_sexp = sexp ?tags ?level (Lazy.force log) v to_sexp
  let of_lazy ?tags ?level l_msg = of_lazy ?tags ?level (Lazy.force log) l_msg
  let message msg = message (Lazy.force log) msg
end

module Blocking : sig
  module Output : sig
    type t

    val create : (Message.t -> unit) -> t
    val screen : t
  end

  val set_level  : Level.t -> unit
  val set_output : Output.t -> unit
  val raw        : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val info       : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val error      : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val debug      : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
end = struct
  module Output = struct
    type t = Message.t -> unit

    let create = ident

    module Screen : sig
      val write : (Message.t -> unit)
    end = struct
      let write = (fun msg ->
        match Message.level msg with
        | None ->
          Printf.printf "%s\n%!" (Message.to_write_only_text msg);
        | Some l ->
          if l = `Error then
            Printf.eprintf "%s\n%!" (Message.to_write_only_text msg)
          else
            Printf.printf "%s\n%!" (Message.to_write_only_text msg))
      ;;
    end

    let screen = Screen.write
  end

  let level : Level.t ref = ref `Info
  let write = ref Output.Screen.write

  let set_level l = level := l

  let set_output output = write := output

  let write msg =
    if Scheduler.is_running () then
      failwith "Log.Global.Blocking function called after scheduler started";
    match Message.level msg with
    | None   -> !write msg
    | Some l ->
      if Level.equal_or_more_verbose_than !level l then !write msg
  ;;

  let gen ?(tags=[]) level k =
    ksprintf (fun msg ->
      let msg = lazy msg in
      write (Message.create level ~tags msg)) k
  ;;

  let raw   ?tags k = gen ?tags None k
  let debug ?tags k = gen ?tags (Some `Debug) k
  let info  ?tags k = gen ?tags (Some `Info) k
  let error ?tags k = gen ?tags (Some `Error) k
end

(* Programs that want simplistic single-channel logging can open this module.  It provides
   a global logging facility to a single output type at a single level. *)
module Global = Make_global(struct end)

module Reader = struct
  let pipe format filename =
    let pipe_r, pipe_w = Pipe.create () in
    don't_wait_for (Reader.with_file filename ~f:(fun r ->
      match format with
      | `Sexp     ->
        let sexp_pipe = Reader.read_sexps r in
        Pipe.transfer sexp_pipe pipe_w ~f:Message.t_of_sexp
        >>| fun () ->
        Pipe.close pipe_w
      | `Bin_prot ->
        let rec loop () =
          Reader.read_bin_prot r Message.bin_reader_t
          >>= function
          | `Eof    ->
            Pipe.close pipe_w;
            Deferred.unit
          | `Ok msg ->
            Pipe.write pipe_w msg
            >>= loop
        in
        loop ()));
    pipe_r
  ;;
end
