
open Core.Std
open Import

include Core.Std.Command


let in_async spec =
  Spec.wrap
    (fun ~run ~main ->
      let args_applied = run main in
      (fun () ->
        upon (args_applied ()) (fun () -> Shutdown.shutdown 0);
        (never_returns (Scheduler.go ()) : unit)))
    spec
;;

let async_basic ~summary ?readme spec main =
  basic ~summary ?readme (in_async spec) main
;;
