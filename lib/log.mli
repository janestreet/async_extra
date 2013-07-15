(**
  A library for general logging.

  Although this module is fully async-safe it exposes almost no Deferreds.  This is
  partially a design choice to minimize the impact of logging in code, and partially the
  result of organic design (i.e. older versions of this interface did the same thing).

  A (limited) Blocking module is supplied in Global to accomodate the portion of a program
  that runs outside of Async.
*)
open Core.Std
open Import

module Level : sig
  (* Describes both the level of a log and the level of a message sent to a log.  There is
     an ordering to levels (`Debug < `Info < `Error), and a log set to a level will never
     display messages at a lower log level. *)
  type t = [
    | `Debug
    | `Info  (* default level *)
    | `Error ]
    with sexp

  include Stringable with type t := t
end

module Message : sig
  type t with sexp, bin_io

  val time    : t -> Time.t
  val message : t -> string
  val level   : t -> Level.t option
  val tags    : t -> (string * string) list
end

module Rotation : sig
  (* description of boundaries for file rotation.

     If all fields are None the file will never be rotated.  Any field set to Some _ will
     cause rotation to happen when that boundary is crossed.  Multiple boundaries may be
     set.  Log rotation always causses incrementing rotation conditions (e.g. size) to
     reset.

     The condition [keep] is special and does not follow the rules above.  When a log is
     rotated [keep] is examined and logs that do not fall under its instructions are
     deleted.  This deletion takes place on rotation only, and so may not happen.  The
     meaning of keep options are:
       | `All -> never delete
       | `Newer_than span ->
         delete files with a timestamp older than (Time.sub (Time.now ()) span).  This
         normally means keeping files that contain at least one message logged within
         span.  If span is short enough this option can delete a just rotated file.
       | `At_least i -> keep the i most recent files

    WARNING: The rotating file functionality of Log is the most poorly tested, and in many
    ways the most complex.  Before using this mode in anger you should test failure cases
    carefully.
  *)
  type t = {
    messages      : int option;
    size          : Byte_units.t option;
    time          : (Time.Ofday.t * Zone.t) option;
    keep          : [ `All | `Newer_than of Time.Span.t | `At_least of int ];
    naming_scheme : [ `Numbered | `Timestamped ]
  } with sexp
end

module Output : sig
  type machine_readable_format = [`Sexp | `Bin_prot ]
  type format = [ machine_readable_format | `Text ]

  type t

  (** [create f] returns a t, given a function that actually performs the final output
      work.  It is the responsibility of the write function to contain all state, and to
      clean up after itself when it is garbage collected (which may require a finalizer).

      The "stock" output modules support a sexp and bin_prot output format, and other
      output modules should make efforts to support them as well where it is
      meaningful/appropriate to do so.

      The unit Deferred returned by the function should not be fulfilled until the all of
      the messages in the given queue are completely handled (e.g. written to disk).
  *)
  val create : (Message.t Queue.t -> unit Deferred.t) -> t

  val stdout        : unit -> t
  val stderr        : unit -> t
  (* see Async_extended.Syslog for syslog output *)
  val writer        : format -> Writer.t -> t
  val file          : format -> filename:string -> t
  val rotating_file : format -> basename:string -> Rotation.t -> t
end

module Blocking : sig
  (* Async programs often have a non-async portion that runs before the scheduler begins
      to capture command line options, do setup, read configs, etc.  This module provides
      limited global logging functions to be used during that period.  Calling these
      functions after the scheduler has started will raise an exception.  They otherwise
      behave similarly to the logging functions in the async world. *)

  module Output : sig
    type t

    val stdout : t
    val stderr : t
    (* see Async_extended.Syslog for syslog output *)

    val create : (Message.t -> unit) -> t
  end

  val set_level : Level.t -> unit
  val set_output : Output.t -> unit

  (** [raw] printf like logging for raw (no level) messages.  Raw messages are still
      output with a timestamp. *)
  val raw   : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a

  (** [info] printf like logging at the `Info log level *)
  val info  : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a

  (** [error] printf like logging at the `Info log level *)
  val error : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a

  (** [error] printf like logging at the `Info log level *)
  val debug : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
end

type t

(* An interface for singleton logs *)
module type Global_intf = sig
  val log : t Lazy.t

  val set_level : Level.t -> unit
  val set_output : Output.t list -> unit

  (** logging functions as the functions that operate on a given log.  In this case they
      operate on a single log global to the module *)
  val raw   : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val info  : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val error : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val debug : ?tags:(string * string) list -> ('a, unit, string, unit) format4 -> 'a
  val flushed : unit -> unit Deferred.t

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

(* This functor can be called to generate "singleton" logging modules *)
module Make_global(Empty : sig end) : Global_intf

(* Programs that want simplistic single-channel logging can open this module.  It
   provides a global logging facility to a single output type at a single level.
   More nuanced logging can be had by using the functions that operate on a
   distinct Log.t type. *)
module Global : Global_intf

(** [set_level] sets the level of the given log.  Messages sent at a level less than the
    current level will not be output. *)
val set_level : t -> Level.t -> unit

(** [set_output] changes the output type of the log, which can be useful when daemonizing.
    The new output type will be applied to all subsequent messages. *)
val set_output : t -> Output.t list -> unit

(** [close] closes a log so that further write attempts will raise an error. *)
val close : t -> unit

(** [flushed] returns a Deferred.t that is fulfilled when the last message delivered to t
    before the call to flushed is out the door. *)
val flushed : t -> unit Deferred.t

(** [create] create a new log *)
val create : level:Level.t -> output:Output.t list -> t

(** [raw] printf like logging for raw (no level) messages.  Raw messages are still
    output with a timestamp. *)
val raw   : ?tags:(string * string) list -> t -> ('a, unit, string, unit) format4 -> 'a

(** [debug] printf like logging at the `Debug log level *)
val debug : ?tags:(string * string) list -> t -> ('a, unit, string, unit) format4 -> 'a

(** [info] printf like logging at the `Info log level *)
val info  : ?tags:(string * string) list -> t -> ('a, unit, string, unit) format4 -> 'a

(** [error] printf like logging at the `Error log level *)
val error : ?tags:(string * string) list -> t -> ('a, unit, string, unit) format4 -> 'a

(** [printf] generalized printf style logging *)
val printf
  :  ?tags:(string * string) list
  -> ?level:Level.t
  -> t
  -> ('a, unit, string, unit) format4
  -> 'a

(** [sexp] logging of values without first converting them to a string.  In the case
    where the log level would discard this message no conversion will ever be done. *)
val sexp
  :  ?tags:(string * string) list
  -> ?level:Level.t
  -> t
  -> 'a
  -> ('a -> Sexp.t)
  -> unit

(** [of_lazy] logging of lazy values.  In the case where the log level would discard this
    message no evaluation will ever be forced. *)
val of_lazy
  :  ?tags:(string * string) list
  -> ?level:Level.t
  -> t
  -> string Lazy.t
  -> unit

val message : t -> Message.t -> unit

module Reader : sig
  (** [pipe format filename] returns a pipe of all the messages in the log.  Errors
      encountered when opening or reading the file will be thrown as exceptions into the
      monitor current at the time pipe is called. *)
  val pipe
    :  [< Output.machine_readable_format ]
    -> string
    -> Message.t Pipe.Reader.t
end
