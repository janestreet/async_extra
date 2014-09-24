
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

  let all = [`Debug; `Info; `Error]

  let arg =
    Command.Spec.Arg_type.of_alist_exn
      (List.map all ~f:(fun t -> (String.lowercase (to_string t), t)))
  ;;

  (* Ordering of log levels in terms of verbosity. *)
  let as_or_more_verbose_than ~log_level ~msg_level =
    match msg_level with
    | None           -> true
    | Some msg_level ->
      begin match log_level, msg_level with
      | `Error, `Error           -> true
      | `Error, (`Debug | `Info) -> false
      | `Info,  (`Info | `Error) -> true
      | `Info, `Debug            -> false
      | `Debug, _                -> true
      end
  ;;
end

module Rotation = struct
  (* description of boundaries for file rotation.  If all fields are None the file will
     never be rotated.  Any field set to Some _ will cause rotation to happen when that
     boundary is crossed.  Multiple boundaries may be set.  Log rotation always causes
     incrementing rotation conditions (e.g. size) to reset, though this is the
     responsibililty of the caller to should_rotate.
  *)
  module V1 = struct
    type t = {
      messages      : int option;
      size          : Byte_units.t option;
      time          : (Time.Ofday.t * Time.Zone.t) option;
      keep          : [ `All | `Newer_than of Time.Span.t | `At_least of int ];
      naming_scheme : [ `Numbered | `Timestamped ];
    } with sexp, fields
  end

  module V2 = struct
    type t = {
      messages      : int sexp_option;
      size          : Byte_units.t sexp_option;
      time          : Time.Ofday.t sexp_option;
      keep          : [ `All | `Newer_than of Time.Span.t | `At_least of int ];
      naming_scheme : [ `Numbered | `Timestamped | `Dated ];
      zone          : Time.Zone.t with default(Time.Zone.machine_zone ());
    } with sexp, fields

    let of_v1 { V1. messages; size; time; keep; naming_scheme } =
      let time, zone =
        match time with
        | None -> None, Time.Zone.machine_zone ()
        | Some (ofday, zone) -> Some ofday, zone
      in
      let naming_scheme = (naming_scheme :> [ `Numbered | `Timestamped | `Dated ]) in
      { messages; size; time; keep; naming_scheme; zone }
  end

  include V2

  let create ?messages ?size ?time ?zone ~keep ~naming_scheme () =
    { messages
    ; size
    ; time
    ; zone = Option.value zone ~default:(Time.Zone.machine_zone ())
    ; keep
    ; naming_scheme
    }

  let t_of_sexp sexp =
    try V2.t_of_sexp sexp
    with v2_exn ->
      try V2.of_v1 (V1.t_of_sexp sexp)
      with v1_exn ->
        Error.raise
          (Error.tag
             (Error.of_list [Error.of_exn v2_exn; Error.of_exn v1_exn])
             "Couldn't parse V1 and V2 log rotation sexp")

  let sexp_of_t = V2.sexp_of_t

  let first_occurrence_after time ~ofday ~zone =
    let first_at_or_after time = Time.occurrence `First_after_or_at time ~ofday ~zone in
    let candidate = first_at_or_after time in
    (* we take care not to return the same time we were given *)
    if Time.equal time candidate
    then first_at_or_after (Time.add time Time.Span.robust_comparison_tolerance)
    else candidate
  ;;

  let should_rotate t ~last_messages ~last_size ~last_time ~current_time =
    Fields.fold ~init:false
      ~messages:(fun acc field ->
        match Field.get field t with
        | None -> acc
        | Some rotate_messages -> acc || rotate_messages <= last_messages)
      ~size:(fun acc field ->
        match Field.get field t with
        | None -> acc
        | Some rotate_size -> acc || Byte_units.(<=) rotate_size last_size)
      ~time:(fun acc field ->
        match Field.get field t with
        | None -> acc
        | Some rotation_ofday ->
          let rotation_time =
            first_occurrence_after last_time ~ofday:rotation_ofday ~zone:t.zone
          in
          acc || current_time >= rotation_time)
      ~zone:(fun acc _ -> acc)
      ~keep:(fun acc _ -> acc)
      ~naming_scheme:(fun acc _ -> acc)
  ;;

  let default ?(zone=Time.Zone.machine_zone ()) () =
    { messages = None
    ; size = None
    ; time = Some Time.Ofday.start_of_day
    ; keep = `All
    ; naming_scheme = `Dated
    ; zone
    }
end

module Sexp_or_string = struct
  type t =
    | Sexp of Sexp.t
    | String of string
  with bin_io, sexp

  let to_string = function
    | Sexp sexp  -> Sexp.to_string sexp
    | String str -> str
end
open Sexp_or_string

module Message : sig
  type t with sexp, bin_io

  val create
    :  ?level:Level.t
    -> ?time:Time.t
    -> ?tags:(string * string) list
    -> Sexp_or_string.t
    -> t

  val time     : t -> Time.t
  val level    : t -> Level.t option
  val message  : t -> string
  val tags     : t -> (string * string) list
  val add_tags : t -> (string * string) list -> t

  val to_write_only_text : ?zone:Time.Zone.t -> t -> string

  val write_write_only_text : t -> Writer.t -> unit
  val write_sexp            : t -> hum:bool -> Writer.t -> unit
  val write_bin_prot        : t -> Writer.t -> unit

  module Stable : sig
    module V0 : sig
      type nonrec t = t with sexp, bin_io
    end

    module V2 : sig
      type nonrec t = t with sexp, bin_io
    end
  end
end = struct
  module T = struct
    type 'a t = {
      time    : Time.t;
      level   : Level.t option;
      message : 'a;
      tags    : (string * string) list;
    } with sexp, bin_io

    let (=) t1 t2 =
      let compare_tags =
        Tuple.T2.compare ~cmp1:String.compare ~cmp2:String.compare
      in
      Time.(=.) t1.time t2.time
      && t1.level = t2.level
      && t1.message = t2.message
      (* The same key can appear more than once in tags, and order shouldn't matter
          when comparing *)
      && List.Assoc.compare String.compare String.compare
          (List.sort ~cmp:compare_tags t1.tags) (List.sort ~cmp:compare_tags t2.tags)
        = 0
    ;;

    TEST_UNIT =
      let time    = Time.now () in
      let level   = Some `Info in
      let message = "test unordered tags" in
      let t1 = { time; level; message; tags = [ "a", "a1"; "a", "a2"; "b", "b1" ] } in
      let t2 = { time; level; message; tags = [ "a", "a2"; "a", "a1"; "b", "b1" ] } in
      let t3 = { time; level; message; tags = [ "a", "a2"; "b", "b1"; "a", "a1" ] } in
      assert (t1 = t2);
      assert (t2 = t3);
    ;;
  end
  open T

  (* Log messages are stored, starting with V2, as an explicit version followed by the
     message itself.  This makes it easier to move the message format forward while
     still allowing older logs to be read by the new code.

     If you make a new version you must add a version to the Version module below and
     should follow the Make_versioned_serializable pattern.
  *)
  module Stable = struct
    module Version = struct
      type t =
        | V2
      with sexp, bin_io, compare

      let (<>) t1 t2 = compare t1 t2 <> 0
      let to_string t = Sexp.to_string (sexp_of_t t)
    end

    module type Versioned_serializable = sig
      type t with sexp, bin_io

      val version : Version.t
    end

    module Make_versioned_serializable(T : Versioned_serializable) : sig
      type t with sexp, bin_io
    end with type t = T.t = struct
      type t = T.t
      type versioned_serializable = Version.t * T.t with sexp, bin_io

      let t_of_versioned_serializable (version, t) =
        if Version.(<>) version T.version
        then failwithf !"version mismatch %{Version} <> to expected version %{Version}"
               version T.version ()
        else t
      ;;

      let sexp_of_t t =
        sexp_of_versioned_serializable (T.version, t)
      ;;

      let t_of_sexp sexp =
        let versioned_t = versioned_serializable_of_sexp sexp in
        t_of_versioned_serializable versioned_t
      ;;

      include Bin_prot.Utils.Make_binable (struct
        type t = T.t

        module Binable = struct
          type t = versioned_serializable with bin_io
        end

        let to_binable t           = (T.version, t)
        let of_binable versioned_t = t_of_versioned_serializable versioned_t
      end)
    end

    module V2 = Make_versioned_serializable (struct
      type nonrec t = Sexp_or_string.t t with sexp, bin_io

      let version = Version.V2
    end)

    (* this is the serialization scheme in 111.18 and before *)
    module V0 = struct
      type v0_t = string T.t with sexp, bin_io

      let v0_to_v2 (v0_t : v0_t) : V2.t =
        {
          time    = v0_t.time;
          level   = v0_t.level;
          message = String v0_t.message;
          tags    = v0_t.tags;
        }

      let v2_to_v0 (v2_t : V2.t) : v0_t =
        {
          time    = v2_t.time;
          level   = v2_t.level;
          message = Sexp_or_string.to_string v2_t.message;
          tags    = v2_t.tags;
        }

      include Bin_prot.Utils.Make_binable (struct

        module Binable = struct
          type t = v0_t with bin_io
        end

        let to_binable = v2_to_v0
        let of_binable = v0_to_v2

        type t = Sexp_or_string.t T.t
      end)

      let sexp_of_t t    = sexp_of_v0_t (v2_to_v0 t)
      let t_of_sexp sexp = v0_to_v2 (v0_t_of_sexp sexp)

      type t = V2.t
    end
  end

  include Stable.V2

  (* this allows for automagical reading of any versioned sexp, so long as we can always
     lift to a Message.t *)
  let t_of_sexp sexp =
    match sexp with
    | Sexp.List (Sexp.List (Sexp.Atom "time" :: _) :: _) ->
      Stable.V0.t_of_sexp sexp
    | Sexp.List [ (Sexp.Atom _) as version; _ ] ->
      begin match Stable.Version.t_of_sexp version with
      | V2 -> Stable.V2.t_of_sexp sexp
      end
    | _ -> failwithf !"malformed sexp: %{Sexp}" sexp ()
  ;;

  let create
        ?level
        ?(time  = (Time.now ()))
        ?(tags  = [])
        message =
    { time; level; message; tags }
  ;;

  TEST_UNIT =
    let msg =
      create ~level:`Info ~tags:["a", "tag"]
        (String "the quick brown message jumped over the lazy log")
    in
    let v0_sexp = Stable.V0.sexp_of_t msg in
    let v2_sexp = Stable.V2.sexp_of_t msg in
    assert (t_of_sexp v0_sexp = msg);
    assert (t_of_sexp v2_sexp = msg);
  ;;

  TEST_UNIT =
    let msg =
      create ~level:`Info ~tags:["a", "tag"]
        (Sexp (Sexp.List [ Sexp.Atom "foo"; Sexp.Atom "bar" ]))
    in
    let v0_sexp = Stable.V0.sexp_of_t msg in
    let v2_sexp = Stable.V2.sexp_of_t msg in
    assert (t_of_sexp v0_sexp = { msg with message = String "(foo bar)" });
    assert (t_of_sexp v2_sexp = msg);
  ;;

  TEST_UNIT =
    let msg = create ~level:`Info ~tags:[] (String "") in
    match sexp_of_t msg with
    | Sexp.List [ (Sexp.Atom _) as version; _ ] ->
      ignore (Stable.Version.t_of_sexp version)
    | _ -> assert false
  ;;

  let time t    = t.time
  let level t   = t.level
  let message t =
    let module S = Sexp_or_string in
    match t.message with
    | S.String s -> s
    | S.Sexp sexp -> Sexp.to_string_mach sexp
  ;;

  let tags t    = t.tags
  let add_tags t new_tags = { t with tags = List.unordered_append new_tags t.tags }

  let to_write_only_text ?zone t =
    let prefix =
      match t.level with
      | None   -> ""
      | Some l -> Level.to_string l ^ " "
    in
    let formatted_tags =
      match t.tags with
      | [] -> []
      | _ :: _ ->
        " --"
        :: List.concat_map t.tags ~f:(fun (t, v) ->
          [" ["; t; ": "; v; "]"])
    in
    String.concat ~sep:"" (
      Time.to_string_abs ?zone t.time
      :: " "
      :: prefix
      :: message t
      :: formatted_tags)
  ;;

  TEST_UNIT =
    let check expect t =
      let zone = Time.Zone.utc in
      <:test_result<string>> (to_write_only_text ~zone t) ~expect
    in
    check "2013-12-13 15:00:00.000000Z <message>"
      { time = Time.of_string "2013-12-13 15:00:00Z"
      ; level = None
      ; tags = []
      ; message = String "<message>"
      };
    check "2013-12-13 15:00:00.000000Z Info <message>"
      { time = Time.of_string "2013-12-13 15:00:00Z"
      ; level = Some `Info
      ; tags = []
      ; message = String "<message>"
      };
    check "2013-12-13 15:00:00.000000Z Info <message> -- [k1: v1] [k2: v2]"
      { time = Time.of_string "2013-12-13 15:00:00Z"
      ; level = Some `Info
      ; tags = ["k1", "v1"; "k2", "v2"]
      ; message = String "<message>"
      };
  ;;

  let write_write_only_text t wr =
    Writer.write wr (to_write_only_text t);
    Writer.newline wr
  ;;

  let write_sexp t ~hum wr =
    Writer.write_sexp ~hum wr (sexp_of_t t);
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
  type machine_readable_format = [`Sexp | `Sexp_hum | `Bin_prot ] with sexp
  type format = [ machine_readable_format | `Text ] with sexp

  type t with sexp_of

  val create : (Message.t Queue.t -> unit Deferred.t) -> t
  val apply : t -> Message.t Queue.t -> unit Deferred.t

  val stdout        : unit -> t
  val stderr        : unit -> t
  val writer        : format -> Writer.t -> t
  val file          : format -> filename:string -> t
  val rotating_file : format -> basename:string -> Rotation.t -> t

  val combine : t list -> t
end = struct
  type machine_readable_format = [`Sexp | `Sexp_hum | `Bin_prot ] with sexp
  type format = [ machine_readable_format | `Text ] with sexp

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
      Message.write_sexp msg ~hum:false w
    | `Sexp_hum ->
      Message.write_sexp msg ~hum:true w
    | `Bin_prot ->
      Message.write_bin_prot msg w
    | `Text ->
      Message.write_write_only_text msg w
    end;
  ;;

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
      val create : Time.Zone.t -> t
      (* For any rotation scheme that renames logs on rotation, this defines how to do
         the renaming. *)
      val rotate_one : t -> t

      val to_string_opt : t -> string option
      val of_string_opt : string option -> t option
      val cmp_newest_first : t -> t -> int
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

      (* errors from this function should be ignored.  If this function fails to run, the
         disk may fill up with old logs, but external monitoring should catch that, and
         the core function of the Log module will be unaffected. *)
      let maybe_delete_old_logs ~dirname ~basename keep =
        begin match keep with
        | `All -> return []
        | `Newer_than span ->
          current_log_files ~dirname ~basename
          >>= fun files ->
          let cutoff = Time.sub (Time.now ()) span in
          Deferred.List.filter files ~f:(fun (_,filename) ->
            Deferred.Or_error.try_with (fun () -> Unix.stat filename)
            >>| function
            | Error _ -> false
            | Ok stats -> Time.(<) stats.Unix.Stats.mtime cutoff)
        | `At_least i ->
          current_log_files ~dirname ~basename
          >>| fun files ->
          let files =
            List.sort files ~cmp:(fun (i1,_) (i2,_) -> Id.cmp_newest_first i1 i2)
          in
          List.drop files i
        end
        >>= Deferred.List.map ~f:(fun (_i,filename) ->
          Deferred.Or_error.try_with (fun () -> Unix.unlink filename))
        >>| fun (_ : unit Or_error.t list) -> ()
      ;;

      type t =
        {         basename      : string
        ;         dirname       : string
        ;         rotation      : Rotation.t
        ;         sequencer     : unit Sequencer.t sexp_opaque
        ; mutable filename      : string
        ; mutable last_messages : int
        ; mutable last_size     : int
        ; mutable last_time     : Time.t
        } with sexp

      let rotate t =
        let basename, dirname = t.basename, t.dirname in
        current_log_files ~dirname ~basename
        >>= fun files ->
        List.rev (List.sort files ~cmp:(fun (i1,_) (i2, _) -> Id.cmp_newest_first i1 i2))
        |> Deferred.List.iter ~f:(fun (id, src) ->
          let id' = Id.rotate_one id in
          if Id.cmp_newest_first id id' <> 0
          then Unix.rename ~src ~dst:(make_filename ~dirname ~basename id')
          else Deferred.unit)
        >>= fun () ->
        maybe_delete_old_logs ~dirname ~basename t.rotation.Rotation.keep
        >>| fun () ->
        t.filename <-
          make_filename ~dirname ~basename (Id.create (Rotation.zone t.rotation))
      ;;

      let create format ~basename rotation =
        let basename, dirname =
          (* Fix dirname, because cwd may change *)
          match Filename.is_absolute basename with
          | true  -> Filename.basename basename, return (Filename.dirname basename)
          | false -> basename, Sys.getcwd ()
        in
        let t_deferred =
          dirname
          >>= fun dirname ->
          let sequencer = Sequencer.create () in
          let t =
            { basename
            ; dirname
            ; rotation
            ; sequencer
            ; filename =
                make_filename ~dirname ~basename (Id.create (Rotation.zone rotation))
            ; last_size = 0
            ; last_messages = 0
            ; last_time = Time.now ()
            }
          in
          Throttle.enqueue t.sequencer (fun () -> rotate t)
          >>| fun () -> t
        in
        fun msgs ->
          t_deferred
          >>= fun t ->
          Throttle.enqueue t.sequencer (fun () ->
            let current_time = Time.now () in
            Deferred.Or_error.try_with (fun () ->
              if Rotation.should_rotate
                   rotation
                   ~last_messages:t.last_messages
                   ~last_size:(Byte_units.create `Bytes (Float.of_int t.last_size))
                   ~last_time:t.last_time ~current_time
              then rotate t
              else Deferred.unit)
            (* rotation errors are not worth potentially crashing the process. *)
            >>= fun (_ : unit Or_error.t) ->
            File.write' format ~filename:t.filename msgs
            >>= fun size ->
            t.last_messages <- t.last_messages + Queue.length msgs;
            t.last_size     <- t.last_size + Int63.to_int_exn size;
            t.last_time     <- current_time;
            Deferred.unit
          )
      ;;
    end

    module Numbered = Make (struct
      type t            = int
      let create        = const 0
      let rotate_one    = (+) 1
      let to_string_opt = function 0 -> None | x -> Some (Int.to_string x)
      let cmp_newest_first  = Int.ascending

      let of_string_opt = function
        | None   -> Some 0
        | Some s -> try Some (Int.of_string s) with _ -> None
      ;;
    end)

    module Timestamped = Make (struct
      type t               = Time.t
      let create _zone     = Time.now ()
      let rotate_one       = ident
      let to_string_opt ts = Some (Time.to_filename_string ts)
      let cmp_newest_first     = Time.descending

      let of_string_opt    = function
        | None   -> None
        | Some s -> try Some (Time.of_filename_string s) with _ -> None
      ;;
    end)

    module Dated = Make (struct
      type t = Date.t
      let create zone = Time.to_date (Time.now ()) zone
      let rotate_one = ident
      let to_string_opt date = Some (Date.to_string date)
      let of_string_opt = function
        | None -> None
        | Some str -> Option.try_with (fun () -> Date.of_string str)
      let cmp_newest_first = Date.descending
    end)

    let create format ~basename rotation =
      match rotation.Rotation.naming_scheme with
      | `Numbered    -> Numbered.create format ~basename rotation
      | `Timestamped -> Timestamped.create format ~basename rotation
      | `Dated       -> Dated.create format ~basename rotation
    ;;
  end

  let rotating_file = Rotating_file.create
  let file          = File.create
  let writer        = Writer.create
  let stdout        = Memo.unit (fun () -> Writer.create `Text (Lazy.force Async_unix.Writer.stdout))
  let stderr        = Memo.unit (fun () -> Writer.create `Text (Lazy.force Async_unix.Writer.stderr))
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
    | Msg        of Message.Stable.V2.t
    | New_level  of Level.t
    | New_output of Output.t
    | Flush      of unit Ivar.t
  with sexp_of

  let to_string t = Sexp.to_string (sexp_of_t t)
end

type t =
  { updates : Update.t Pipe.Writer.t
  ; mutable current_level : Level.t }

let equal t1 t2 = Pipe.equal t1.updates t2.updates
let hash t = Pipe.hash t.updates

let sexp_of_t _t = Sexp.Atom "<opaque>"

let push_update t update =
  if not (Pipe.is_closed t.updates)
  then Pipe.write_without_pushback t.updates update
  else
    failwithf "Log: can't process %s because this log has been closed"
      (Update.to_string update) ()
;;

let flushed t = Deferred.create (fun i -> push_update t (Update.Flush i))

let closed t = Pipe.is_closed t.updates

module Flush_at_exit_or_gc : sig
  val add_log : t -> unit
  val close   : t -> unit
end = struct
  module Weak_table = Caml.Weak.Make (struct
    type z = t
    type t = z
    let equal = equal
    let hash  = hash
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
      Pipe.close t.updates
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
  let batch_size    = 100 in
  let write         = ref (Output.combine output) in
  let current_level = ref level in
  let msgs          = Queue.create () in
  let output_message_queue f =
    if Queue.length msgs = 0
    then f ()
    else begin
      (Output.apply !write) msgs
      >>= fun () ->
      Queue.clear msgs;
      f ()
    end
  in
  (fun updates ->
     let rec loop yield_every =
       let yield_every = yield_every - 1 in
       if yield_every = 0
       then begin
         (* this introduces a yield point so that other async jobs have a chance to run
            under circumstances when large batches of logs are delivered in bursts. *)
         Scheduler.yield ()
         >>= fun () ->
         loop batch_size
       end else begin
         match Queue.dequeue updates with
         | None        ->
           output_message_queue (fun _ -> Deferred.unit)
         | Some update ->
           match update with
           | Update.Flush i ->
             output_message_queue (fun () ->
               Ivar.fill i ();
               loop yield_every)
           | Update.Msg msg ->
             Queue.enqueue msgs msg;
             loop yield_every
           | Update.New_level level ->
             current_level := level;
             loop yield_every
           | Update.New_output f ->
             output_message_queue (fun () ->
               write := f;
               loop yield_every)
       end
     in
     loop batch_size)
;;

let create ~level ~output : t =
  let r,w         = Pipe.create () in
  let process_log = create_log_processor ~level ~output in
  don't_wait_for (Pipe.iter' r ~f:process_log);
  let t =
    { updates = w
    ; current_level = level }
  in
  Flush_at_exit_or_gc.add_log t;
  t
;;

let set_output t outputs =
  push_update t (Update.New_output (Output.combine outputs))
;;

let level t = t.current_level

let set_level t level =
  t.current_level <- level;
  push_update t (Update.New_level level)
;;

TEST_UNIT "Level setting" =
  let assert_log_level log expected_level =
    match (level log, expected_level) with
    | `Info, `Info
    | `Debug, `Debug
    | `Error, `Error -> Ok ()
    | actual_level, expected_level ->
      Or_error.errorf "Expected %S but got %S"
        (Level.to_string expected_level) (Level.to_string actual_level)
  in
  let answer =
    let open Or_error.Monad_infix in
    let initial_level = `Debug in
    let output = [Output.create (fun _ -> Deferred.unit)] in
    let log = create ~level:initial_level ~output in
    assert_log_level log initial_level
    >>= fun () ->
    set_level log `Info;
    assert_log_level log `Info
    >>= fun () ->
    set_level log `Debug;
    set_level log `Error;
    assert_log_level log `Error
  in
  Or_error.ok_exn answer
;;

let message t msg = push_update t (Update.Msg msg)

let printf ?level:msg_level ?time ?tags t k =
  ksprintf (fun msg ->
    if Level.as_or_more_verbose_than ~log_level:(level t) ~msg_level
    then begin
      Message.create ?level:msg_level ?time ?tags (String msg)
      |> message t;
    end
  )
    k
;;

let sexp ?level:msg_level ?time ?tags t v to_sexp =
  if Level.as_or_more_verbose_than ~log_level:(level t) ~msg_level
  then begin
    Message.create ?level:msg_level ?time ?tags (Sexp (to_sexp v))
    |> message t;
  end
;;

let string ?level:msg_level ?time ?tags t s =
  if Level.as_or_more_verbose_than ~log_level:(level t) ~msg_level
  then begin
    Message.create ?level:msg_level ?time ?tags (String s)
    |> message t;
  end
;;

let raw   ?time ?tags t k = printf ?time ?tags t k
let debug ?time ?tags t k = printf ~level:`Debug ?time ?tags t k
let info  ?time ?tags t k = printf ~level:`Info  ?time ?tags t k
let error ?time ?tags t k = printf ~level:`Error ?time ?tags t k

module type Global_intf = sig
  val log : t Lazy.t

  val level      : unit -> Level.t
  val set_level  : Level.t -> unit
  val set_output : Output.t list -> unit
  val raw        : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val info       : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val error      : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val debug      : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val flushed    : unit -> unit Deferred.t

  val printf
    :  ?level:Level.t
    -> ?time:Time.t
    -> ?tags:(string * string) list
    -> ('a, unit, string, unit) format4
    -> 'a

  val sexp
    :  ?level:Level.t
    -> ?time:Time.t
    -> ?tags:(string * string) list
    -> 'a
    -> ('a -> Sexp.t)
    -> unit

  val string
    :  ?level:Level.t
    -> ?time:Time.t
    -> ?tags:(string * string) list
    -> string
    -> unit

  val message : Message.t -> unit
end

module Make_global(Empty : sig end) : Global_intf = struct
  let log               = lazy (create ~level:`Info ~output:[Output.stderr ()])
  let level ()          = level (Lazy.force log)
  let set_level level   = set_level (Lazy.force log) level
  let set_output output = set_output (Lazy.force log) output

  let raw   ?time ?tags k = raw   ?time ?tags (Lazy.force log) k
  let info  ?time ?tags k = info  ?time ?tags (Lazy.force log) k
  let error ?time ?tags k = error ?time ?tags (Lazy.force log) k
  let debug ?time ?tags k = debug ?time ?tags (Lazy.force log) k

  let flushed () = flushed (Lazy.force log)
  let printf  ?level ?time ?tags k         = printf ?level ?time ?tags (Lazy.force log) k
  let sexp    ?level ?time ?tags v to_sexp = sexp ?level ?time ?tags (Lazy.force log) v to_sexp
  let string  ?level ?time ?tags s         = string ?level ?time ?tags (Lazy.force log) s
  let message msg = message (Lazy.force log) msg
end

module Blocking : sig
  module Output : sig
    type t

    val create : (Message.t -> unit) -> t
    val stdout : t
    val stderr : t
  end

  val level      : unit -> Level.t
  val set_level  : Level.t -> unit
  val set_output : Output.t -> unit
  val raw        : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val info       : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val error      : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val debug      : ?time:Time.t -> ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
end = struct
  module Output = struct
    type t = Message.t -> unit

    let create = ident

    let write print = (fun msg -> print (Message.to_write_only_text msg))
    let stdout      = write (Printf.printf "%s\n%!")
    let stderr      = write (Printf.eprintf "%s\n%!")
  end

  let level : Level.t ref = ref `Info
  let write = ref Output.stderr

  let set_level l = level := l
  let level () = !level

  let set_output output = write := output

  let write msg =
    if Scheduler.is_running () then
      failwith "Log.Global.Blocking function called after scheduler started";
    !write msg
  ;;

  let gen ?level:msg_level ?time ?tags k =
    ksprintf (fun msg ->
      if Level.as_or_more_verbose_than ~log_level:(level ()) ~msg_level
      then begin
        let msg = String msg in
        write (Message.create ?level:msg_level ?time ?tags msg)
      end) k;
  ;;

  let raw   ?time ?tags k = gen ?time ?tags k
  let debug ?time ?tags k = gen ~level:`Debug ?time ?tags k
  let info  ?time ?tags k = gen ~level:`Info  ?time ?tags k
  let error ?time ?tags k = gen ~level:`Error ?time ?tags k
end

(* Programs that want simplistic single-channel logging can open this module.  It provides
   a global logging facility to a single output type at a single level. *)
module Global = Make_global(struct end)

module Reader = struct
  let pipe format filename =
    let pipe_r, pipe_w = Pipe.create () in
    don't_wait_for (Reader.with_file filename ~f:(fun r ->
      match format with
      | `Sexp | `Sexp_hum ->
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
