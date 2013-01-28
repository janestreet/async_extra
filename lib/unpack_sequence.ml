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

let unpack_from_string_pipe unpack_buffer input =
  let output_reader, output_writer = Pipe.create () in
  let result =
    Deferred.repeat_until_finished () (fun () ->
      Pipe.read' input
      >>= function
      | `Eof ->
        return
          (`Finished
              (match Unpack_buffer.is_empty unpack_buffer with
              | Error error -> Result.Unpack_error error
              | Ok true -> Result.Input_closed
              | Ok false -> Result.Input_closed_in_the_middle_of_data unpack_buffer))
      | `Ok q ->
        let feed_result =
          with_return (fun r ->
            Queue.iter q ~f:(fun string ->
              match Unpack_buffer.feed_string unpack_buffer string with
              | Ok () -> ()
              | Error error -> r.return (Error error));
            Unpack_buffer.unpack unpack_buffer)
        in
        match feed_result with
        | Error error -> return (`Finished (Result.Unpack_error error))
        | Ok outputs ->
          if Pipe.is_closed output_writer then
            return (`Finished (Result.Output_closed (outputs, unpack_buffer)))
          else if Queue.is_empty outputs then
            return (`Repeat ())
          else begin
            Pipe.write' output_writer outputs
            >>| fun () ->
            `Repeat ()
          end)
  in
  output_reader, result
;;

let unpack_from_reader unpack_buffer reader =
  let pipe_r, pipe_w = Pipe.create () in
  let stop a = return (`Stop a) in
  let handle_chunk buf ~pos ~len =
    match Unpack_buffer.feed unpack_buffer buf ~pos ~len with
    | Error error -> stop (Result.Unpack_error error)
    | Ok () ->
      match Unpack_buffer.unpack unpack_buffer with
      | Error error -> stop (Result.Unpack_error error)
      | Ok output_queue ->
        if Pipe.is_closed pipe_w then
          stop (Result.Output_closed (output_queue, unpack_buffer))
        else if Queue.is_empty output_queue then
          return `Continue
        else begin
          Pipe.write' pipe_w output_queue
          >>| fun () ->
          `Continue
        end
  in
  let result =
    Reader.read_one_chunk_at_a_time_until_eof reader ~handle_chunk
    >>| fun res ->
    Pipe.close pipe_w;
    match res with
    | `Stopped result -> result
    | `Eof ->
      match Unpack_buffer.is_empty unpack_buffer with
      | Error error -> Result.Unpack_error error
      | Ok true -> Result.Input_closed
      | Ok false -> Result.Input_closed_in_the_middle_of_data unpack_buffer
  in
  (pipe_r, result)
;;

let unpack_bin_prot_from_reader bin_prot_reader reader =
  unpack_from_reader (Unpack_buffer.create_bin_prot bin_prot_reader) reader
;;
