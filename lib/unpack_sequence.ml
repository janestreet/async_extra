open Core.Std
open Import

module Result = struct
  type ('a, 'b) t =
  | Input_closed
  | Input_closed_in_the_middle_of_data of ('a, 'b) Unpack_buffer.t
  | Output_closed of 'a Queue.t * ('a, 'b) Unpack_buffer.t
  | Unpack_error of Error.t
  with sexp_of

  let to_error = function
    | Input_closed -> Error.of_string "input closed"
    | Input_closed_in_the_middle_of_data _ ->
      Error.of_string "input closed in the middle of data"
    | Output_closed _ -> Error.of_string "output closed"
    | Unpack_error error -> Error.create "unpack error" error <:sexp_of< Error.t >>
  ;;
end

let eof unpack_buffer =
  match Unpack_buffer.is_empty unpack_buffer with
  | Error error -> Result.Unpack_error error
  | Ok true     -> Result.Input_closed
  | Ok false    -> Result.Input_closed_in_the_middle_of_data unpack_buffer
;;

let handle_unpack unpack_result unpack_buffer output_writer =
  match unpack_result with
  | Error error -> return (`Stop (Result.Unpack_error error))
  | Ok outputs ->
    if Pipe.is_closed output_writer
    then return (`Stop (Result.Output_closed (outputs, unpack_buffer)))
    else if Queue.is_empty outputs
    then return `Continue
    else Pipe.write' output_writer outputs >>| fun () -> `Continue
;;

let unpack_from_string_pipe unpack_buffer input =
  let output_reader, output_writer = Pipe.create () in
  let result =
    Deferred.repeat_until_finished () (fun () ->
      Pipe.read' input
      >>= function
      | `Eof -> return (`Finished (eof unpack_buffer))
      | `Ok q ->
        let unpack_result =
          with_return (fun r ->
            Queue.iter q ~f:(fun string ->
              match Unpack_buffer.feed_string unpack_buffer string with
              | Ok () -> ()
              | Error error -> r.return (Error error));
            Unpack_buffer.unpack unpack_buffer)
        in
        handle_unpack unpack_result unpack_buffer output_writer
        >>| function
        | `Continue -> `Repeat ()
        | `Stop z -> `Finished z)
    >>| fun res ->
    Pipe.close output_writer;
    res
  in
  output_reader, result
;;

let unpack_from_reader unpack_buffer reader =
  let output_reader, output_writer = Pipe.create () in
  let stop a = return (`Stop a) in
  let handle_chunk buf ~pos ~len =
    match Unpack_buffer.feed unpack_buffer buf ~pos ~len with
    | Error error -> stop (Result.Unpack_error error)
    | Ok () ->
      handle_unpack (Unpack_buffer.unpack unpack_buffer) unpack_buffer output_writer
  in
  let result =
    (* In rare situations, a reader can asynchronously raise.  We'd rather not raise here,
       since we have a natural place to report the error in [result] itself. *)
    try_with (fun () -> Reader.read_one_chunk_at_a_time_until_eof reader ~handle_chunk)
    >>| fun res ->
    Pipe.close output_writer;
    match res with
    | Error exn -> Result.Unpack_error (Error.of_exn exn)
    | Ok (`Stopped result) -> result
    | Ok `Eof  -> eof unpack_buffer
  in
  (output_reader, result)
;;

let unpack_bin_prot_from_reader bin_prot_reader reader =
  unpack_from_reader (Unpack_buffer.create_bin_prot bin_prot_reader) reader
;;
