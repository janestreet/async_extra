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

module Nfs = struct
  let get_hostname_and_pid path =
    In_thread.run (fun () -> Core.Std.Lock_file.Nfs.get_hostname_and_pid path)
  ;;

  let get_message path =
    In_thread.run (fun () -> Core.Std.Lock_file.Nfs.get_message path)
  ;;

  let unlock_safely path =
    In_thread.run (fun () -> Core.Std.Lock_file.Nfs.unlock_safely path)
  ;;

  let create ?message path =
    In_thread.run (fun () -> Core.Std.Lock_file.Nfs.create ?message path)
  ;;

  let create_exn ?message path =
    create ?message path
    >>| fun b ->
    if not b then failwiths "Lock_file.Nfs.create" path <:sexp_of< string >>
  ;;

  let waiting_create ?message path =
    Deferred.repeat_until_finished () (fun () ->
      create ?message path
      >>= function
        | true -> return (`Finished ())
        | false -> after (sec 1.) >>| fun () -> `Repeat ())
  ;;

  let critical_section ?message path ~f =
    create_exn ?message path
    >>= fun () ->
    Monitor.protect f ~finally:(fun () -> unlock_safely path)
  ;;
end
