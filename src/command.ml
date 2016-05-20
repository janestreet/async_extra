open Core.Std
open Import

include Core.Std.Command

let shutdown exit_code =
  don't_wait_for begin
    (* Although [Writer.std{out,err}] have [at_shutdown] handlers that flush them, those
       handlers have a 5s timeout that proceeds even if the data hasn't made it to the OS.
       And, there is a 10s timeout in [Shutdown.shutdown] that causes it to proceed even
       if the [at_shutdown] handlers don't finish.  Those timeouts don't play well with
       applications that output a bunch of data to std{out,err} and then exit, when the
       data consumer (e.g. less) hasn't read all the data.  So, before we call
       [Shutdown.shutdown], we ensure that stdout and stderr are flushed with no timeout,
       or that their consumer has left. *)
    Deferred.List.iter ~how:`Parallel
      Writer.([ force stdout; force stderr ])
      ~f:(fun writer ->
        if Writer.is_closed writer
        then return ()
        else Deferred.any_unit [ Writer.close_finished writer
                               ; Writer.consumer_left writer
                               ; Writer.flushed writer
                               ])
    >>| fun () ->
    Shutdown.shutdown exit_code
  end;
;;

let maybe_print_error_and_shutdown = function
  | Ok () -> shutdown 0
  | Error e ->
    prerr_endline (Error.to_string_hum e);
    shutdown 1
;;

let in_async ?extract_exn param on_result =
  Param.map param ~f:(fun main () ->
    upon (Deferred.Or_error.try_with ?extract_exn main) on_result;
    (never_returns (Scheduler.go ()) : unit)
  )
;;

let async' ?extract_exn ~summary ?readme param =
  let on_result = maybe_print_error_and_shutdown in
  basic' ~summary ?readme (in_async ?extract_exn param on_result)
;;

let async ?extract_exn ~summary ?readme spec main =
  async' ?extract_exn ~summary ?readme (Spec.to_param spec main)

let async_or_error' ?extract_exn ~summary ?readme param =
  let on_result res = maybe_print_error_and_shutdown (Or_error.join res) in
  basic' ~summary ?readme (in_async ?extract_exn param on_result)
;;

let async_or_error ?extract_exn ~summary ?readme spec main =
  async_or_error' ?extract_exn ~summary ?readme (Spec.to_param spec main)

let async_basic = async
