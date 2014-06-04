
open Core.Std
open Import

include Core.Std.Command


let in_async spec on_result =
  Spec.wrap
    (fun ~run ~main ->
      let args_applied = run main in
      (fun () ->
        upon (args_applied ()) on_result;
        (never_returns (Scheduler.go ()) : unit)))
    spec
;;

let async ~summary ?readme spec main =
  let on_result () = Shutdown.shutdown 0 in
  basic ~summary ?readme (in_async spec on_result) main
;;

let async_or_error ~summary ?readme spec main =
  let on_result = function
    | Ok () -> Shutdown.shutdown 0
    | Error error ->
      prerr_endline (Error.to_string_hum error);
      Shutdown.shutdown 1
  in
  basic ~summary ?readme (in_async spec on_result) main
;;

