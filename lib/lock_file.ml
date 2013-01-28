open Core.Std
open Import

let create ?message ?close_on_exec ?unlink_on_exit path =
  In_thread.run (fun () ->
    Core.Std.Lock_file.create ?message ?close_on_exec ?unlink_on_exit path)
;;

let create_exn ?message ?close_on_exec ?unlink_on_exit path =
  create ?message ?close_on_exec ?unlink_on_exit path
  >>| fun b ->
  if not b then failwiths "Lock_file.create" path <:sexp_of< string >>
;;

let waiting_create ?message ?close_on_exec ?unlink_on_exit path =
  In_thread.run (fun () ->
    Core.Std.Lock_file.blocking_create
      ?message ?close_on_exec ?unlink_on_exit path)
;;

let is_locked path = In_thread.run (fun () -> Core.Std.Lock_file.is_locked path)
