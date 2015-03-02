open Core.Std
open Async.Std
open Rpc

module Pipe_simple_test = struct
  module String_pipe = struct
    module Query = struct

      type t' =
        { msg_size : Byte_units.t;
          msgs_per_sec : int
        } with bin_io, sexp

      type t = t' option with bin_io, sexp

      let start ~aborted t =
        let bytes =
          match t with
          | None -> 0
          | Some t -> Float.to_int (Byte_units.bytes t.msg_size)
        in
        let reader, writer = Pipe.create () in
        let total_msgs = ref 0 in
        let start = Time.now () in
        Pipe.set_size_budget reader 1000;
        let string = String.init bytes ~f:(fun _ -> 'A') in
        let stop = Deferred.any [ Pipe.closed writer; aborted ] in
        Clock.every ~stop (Time.Span.of_sec 1.) (fun () ->
          Log.Global.printf "Queue size: %d" (Pipe.length reader));
        Clock.every ~stop (Time.Span.of_sec 1.) (fun () ->
          Log.Global.printf "Messages per sec: %f"
            ((Float.of_int !total_msgs) /.
            (Time.Span.to_sec (Time.diff (Time.now ()) start))));
        let prev = ref (Time.now ()) in
        let () =
          match t with
          | None ->
            let rec loop () =
              Pipe.pushback writer
              >>> fun () ->
              Pipe.write_without_pushback writer string;
              loop ()
            in
            loop ()
          | Some t ->
            Clock.every' ~stop (Time.Span.of_sec 1.) (fun () ->
              let msgs =
                let new_time = Time.now () in
                let diff = Time.Span.to_sec (Time.diff new_time !prev) in
                Log.Global.printf "The diff is %f\n" diff;
                prev := new_time;
                Int.of_float (diff *. (Int.to_float t.msgs_per_sec))
              in
              if not (Pipe.is_closed writer) then begin
                for i = 1 to msgs do
                  let _ = i in
                  incr total_msgs;
                  Pipe.write_without_pushback writer string
                done
              end;
              Pipe.downstream_flushed writer
              >>| function
                | `Reader_closed
                | `Ok -> () )
        in
        reader

      let create msg_size msgs_per_sec =
        { msg_size;
          msgs_per_sec
        }

    end

    let rpc =
      Pipe_rpc.create
        ~client_pushes_back:()
        ~name:"test-pipe-rpc"
        ~version:1
        ~bin_query:Query.bin_t
        ~bin_response:String.bin_t
        ~bin_error:Nothing.bin_t
        ()
  end

  module Memory_consumption = struct
    let init () =
      let major_cycles = ref 0 in
      ignore (Gc.Alarm.create (fun () -> incr major_cycles));
      Clock.every (Time.Span.of_sec 5.) (fun () ->
        Log.Global.printf "%d major cycles" !major_cycles)
  end

  module Client = struct

    let _is_the_right_string msg_size string =
      ((String.length string) = msg_size)
      && (String.for_all string ~f:((=) 'A'))

    let main msg_size msg_units msgs_per_sec host port () =
      Memory_consumption.init ();
      let bytes =
        match msg_units with
        | None -> Byte_units.create `Bytes 0.
        | Some msg_units ->
          Byte_units.create (Byte_units.Measure.t_of_sexp (Sexp.of_string msg_units))
            msg_size
      in
      let query = Option.map msgs_per_sec ~f:(String_pipe.Query.create bytes) in
      Connection.client ~host ~port ()
      >>| Result.ok_exn
      >>= fun connection ->
      Pipe_rpc.dispatch String_pipe.rpc connection query
      >>| Or_error.ok_exn
      >>= function
      | Error t -> Nothing.unreachable_code t
      | Ok (pipe, _) ->
        let msgs = ref 0 in
        let start = Time.now () in
        let _msg_size = Int.of_float (Byte_units.bytes bytes) in
        Clock.every (Time.Span.of_sec 1.) (fun () ->
          let now = Time.now () in
          let secs = Time.Span.to_sec (Time.diff now start) in
          Log.Global.printf "%f msgs per sec" ((Float.of_int !msgs) /. secs));
        Pipe.iter_without_pushback pipe ~f:(fun _string ->
          incr msgs)

    let command =
      Command.async ~summary:"test client"
        Command.Spec.(
          empty
          +> flag "msg-size" (required float) ~doc:""
          +> flag "size-units" (optional string) ~doc:""
          +> flag "msgs-per-sec" (optional int) ~doc:""
          +> flag "hostname" (required string) ~doc:""
          +> flag "port" (required int) ~doc:"" )
        main
  end

  module Server = struct

    let implementation =
      Pipe_rpc.implement String_pipe.rpc (fun () query ~aborted ->
        return (Ok (String_pipe.Query.start ~aborted query)))

    let main port () =
      Memory_consumption.init ();
      let implementations =
        Implementations.create_exn
          ~implementations:[ implementation ] ~on_unknown_rpc:`Raise
      in
      Connection.serve ~initial_connection_state:(fun _ _ -> ()) ~implementations
        ~where_to_listen:(Tcp.on_port port) ()
      >>= fun (_ : (_, _) Tcp.Server.t) ->  Deferred.never ()

    let command =
      Command.async ~summary:"test server"
        Command.Spec.(
          empty
          +> flag "port" (required int) ~doc:"" )
        main
  end
  let command =
    Command.group
      ~summary:"Simple client and server to quickly check manually that \
                pipe-rpc is working ok "
        [ "server", Server.command; "client", Client.command ]
end

module Pipe_rpc_performance_measurements = struct
  module Protocol = struct
    type query    = unit with bin_io
    type response = unit with bin_io

    let rpc =
      Pipe_rpc.create
        ~client_pushes_back:()
        ~name:"test-rpc-performance"
        ~version:1
        ~bin_query:bin_query
        ~bin_response:bin_response
        ~bin_error:Nothing.bin_t
        ()
  end

  module Client = struct

    let main ~msgs_per_sec:_ port =
      Connection.client ~host:"localhost" ~port ()
      >>| Result.ok_exn
      >>= fun connection ->
      Pipe_rpc.dispatch Protocol.rpc connection ()
      >>| Or_error.ok_exn
      >>= function
      | Error t -> Nothing.unreachable_code t
      | Ok (pipe, _) ->
        let cnt = ref 0 in
        let total_cnt = ref 0 in
        let ratio_acc = ref 0. in
        let percentage_acc = ref 0. in
        let sample_to_collect_and_exit = ref ~-5 in
        don't_wait_for
          ( Pipe.iter_without_pushback (Cpu_usage.samples ()) ~f:(fun percent ->
             let percentage = Percent.to_percentage percent in
             incr sample_to_collect_and_exit;
             if percentage > 100. then begin
               Print.printf "CPU pegged (%f). This test is not good.\n" percentage;
               Shutdown.shutdown 1
             end;
             if !sample_to_collect_and_exit = 10 then begin
               Print.printf "%f (cpu: %f)\n" (!ratio_acc /. 10.) (!percentage_acc /. 10.);
               Shutdown.shutdown 0
             end else if !sample_to_collect_and_exit >= 0 then begin
              if !cnt > 0 then begin
               let ratio = (percentage *. 1_000_000.) /. (Int.to_float !cnt) in
               ratio_acc := !ratio_acc +. ratio;
               percentage_acc := !percentage_acc +. percentage;
              end;
             end;
             cnt := 0));
        Pipe.iter' pipe ~f:(fun queue ->
          let len = Queue.length queue in
          cnt := !cnt + len;
          total_cnt := !total_cnt + len;
          Deferred.unit)

  end

  module Server = struct

    let start_test ~msgs_per_sec ~aborted =
      let reader, writer = Pipe.create () in
      upon aborted (fun () -> Pipe.close writer);
      let granularity =
        if msgs_per_sec >= 1_000. then 0.001
        else if msgs_per_sec >= 100. then 0.01
        else if msgs_per_sec >= 10. then 0.1
        else 1.
      in
      let last_run_time = ref None in
      let near_messages_to_be_sent =
        let size = granularity *. msgs_per_sec in
        let queue = Queue.create () in
        for _i = 1 to (Int.of_float size) do
          Queue.enqueue queue ();
        done;
        queue
      in
      Clock.every (Time.Span.of_sec granularity) ~stop:aborted (fun () ->
        let now = Time.now () in
        let msgs =
          Int.of_float
            ( match !last_run_time with
            | None -> msgs_per_sec *. granularity
            | Some lst_run ->
              let diff = Time.diff now lst_run in
              (Time.Span.to_sec diff) *. msgs_per_sec)
        in
        last_run_time := Some now;
        let existing_queue = Queue.length near_messages_to_be_sent in
        if existing_queue > msgs then begin
          for _i = (msgs - existing_queue) downto 1 do
            Queue.dequeue_exn near_messages_to_be_sent;
          done;
        end else if existing_queue < msgs then begin
          ( for _i = (msgs - existing_queue) downto 1 do
              Queue.enqueue near_messages_to_be_sent ();
            done)
        end;
        Pipe.write_without_pushback' writer near_messages_to_be_sent);
      reader
    ;;

    let implementation msgs_per_sec =
      Pipe_rpc.implement Protocol.rpc (fun () () ~aborted ->
        return (Ok (start_test ~msgs_per_sec ~aborted)))

    let main msgs_per_sec () =
      let implementations =
        Implementations.create_exn
          ~implementations:[ implementation msgs_per_sec ]
          ~on_unknown_rpc:`Raise
      in
      Connection.serve ~initial_connection_state:(fun _ _ -> ()) ~implementations
        ~where_to_listen:Tcp.on_port_chosen_by_os ()
      >>= fun listening_on ->
      let port = Tcp.Server.listening_on listening_on in
      Client.main ~msgs_per_sec port


  end

    let command =
      Command.async ~summary:"test server"
        Command.Spec.(
          empty
          +> flag "msg-per-sec" (required float) ~doc:"")
        Server.main

end

module Rpc_performance_measurements = struct
  module Protocol = struct
    type query    = unit with bin_io
    type response = unit with bin_io

    let rpc =
      Rpc.create
        ~name:"test-rpc-performance"
        ~version:1
        ~bin_query:bin_query
        ~bin_response:bin_response
  end

  module Client = struct

    let main msgs_per_sec port () =
      Connection.client ~host:"localhost" ~port ()
      >>| Result.ok_exn
      >>= fun connection ->
        Clock.every' (sec 1.) (fun () ->
          Deferred.all
            ( List.init msgs_per_sec ~f:(fun (_ : int) ->
                Rpc.dispatch Protocol.rpc connection ()) )
          >>| fun list ->
          if List.exists list ~f:Result.is_error then assert false);
        Deferred.never ()

  end

  module Server = struct

    let cnt = ref 0

    let implementation =
      Rpc.implement Protocol.rpc (fun () () ->
        incr cnt;
        Deferred.unit)

    let main () =
      let implementations =
        Implementations.create_exn
          ~implementations:[ implementation ]
          ~on_unknown_rpc:`Raise
      in
      Connection.serve ~initial_connection_state:(fun _ _ -> ()) ~implementations
        ~where_to_listen:Tcp.on_port_chosen_by_os ()
      >>= fun listening_on ->
      let port = Tcp.Server.listening_on listening_on in
      Print.printf "Port:%d\n" port;
      let ratio_acc = ref 0. in
      let percentage_acc = ref 0. in
      let sample = ref 0 in
      don't_wait_for
        ( Pipe.iter_without_pushback (Cpu_usage.samples ()) ~f:(fun percent ->
           if 0 = !cnt then  ()
           else begin
            let percentage = Percent.to_percentage percent in
            Print.printf "%d %f (cpu: %f)\n"
              !sample
              (!ratio_acc /. (Float.of_int !sample))
              (!percentage_acc /. (Float.of_int !sample));
            if percentage > 100. then begin
              Print.printf "CPU pegged (%f). This test may not good.\n" percentage;
            end else begin
            if !sample >= 0 then begin
              if !cnt > 0 then begin
              let ratio = (percentage *. 1_000_000.) /. (Int.to_float !cnt) in
              ratio_acc := !ratio_acc +. ratio;
              percentage_acc := !percentage_acc +. percentage;
              end;
            end;
            cnt := 0
          end;
            incr sample;
           end;
          ));
      Deferred.never ()


  end

    let server_command =
      Command.async ~summary:"test server"
        Command.Spec.empty
        Server.main

    let client_command =
      Command.async ~summary:"test server"
        Command.Spec.(
          empty
          +> flag "msg-per-sec" (required int) ~doc:""
          +> flag "port" (required int) ~doc:""
        )
        Client.main

end

module Rpc_expert_test = struct
  let rpc ~name =
    Rpc.create ~name ~version:0
      ~bin_query:bin_string ~bin_response:bin_string

  (* names refer to how they're implemented *)
  let raw_rpc = rpc ~name:"raw"
  let normal_rpc = rpc ~name:"normal"

  let the_query = "flimflam"
  let the_response = String.rev the_query

  let main debug () =
    let level = if debug then `Debug else `Error in
    let log = Log.create ~level ~output:[Log.Output.stdout ()] in
    let implementations =
      let handle_raw () ~rpc_tag ~version responder buf ~pos:init_pos ~len =
        Log.debug log "query: %s v%d" rpc_tag version;
        assert (rpc_tag = Rpc.name raw_rpc && version = Rpc.version raw_rpc);
        try
          let pos_ref = ref init_pos in
          let query = String.bin_read_t buf ~pos_ref in
          <:test_result< string >> query ~expect:the_query;
          Log.debug log "query value = %s" query;
          assert (!pos_ref - init_pos = len);
          let new_buf =
            Bin_prot.Utils.bin_dump String.bin_writer_t the_response
          in
          ignore (
            Rpc.Expert.Responder.schedule responder new_buf ~pos:0
              ~len:(Bigstring.length new_buf)
            : [`Connection_closed | `Flushed of unit Deferred.t]
          );
          Deferred.unit
        with
          e ->
          Log.debug log !"got exception: %{Exn#mach}" e;
          Rpc.Expert.Responder.write_error responder (Error.of_exn e);
          Deferred.unit
      in
      Implementations.Expert.create_exn
        ~implementations:[Rpc.implement normal_rpc (fun () query ->
          <:test_result< string >> query ~expect:the_query;
          return the_response)]
        ~on_unknown_rpc:(`Expert handle_raw)
    in
    Connection.serve () ~implementations
      ~initial_connection_state:(fun _ _ -> ())
      ~where_to_listen:Tcp.on_port_chosen_by_os
    >>= fun server ->
    let port = Tcp.Server.listening_on server in
    Deferred.List.iter [raw_rpc; normal_rpc] ~f:(fun rpc ->
      Connection.with_client ~host:"localhost" ~port (fun conn ->
        Log.debug log "sending %s query normally" (Rpc.name rpc);
        Rpc.dispatch_exn rpc conn the_query
        >>= fun response ->
        Log.debug log "got response";
        <:test_result< string >> response ~expect:the_response;
        let buf = Bin_prot.Utils.bin_dump String.bin_writer_t the_query in
        Log.debug log "sending %s query via Expert interface" (Rpc.name rpc);
        Deferred.create (fun i ->
          ignore (
            Rpc.Expert.schedule_dispatch conn ~rpc_tag:(Rpc.name rpc)
              ~version:(Rpc.version rpc) buf ~pos:0 ~len:(Bigstring.length buf)
              ~handle_error:(fun e -> Ivar.fill i (Error e))
              ~handle_response:(fun buf ~pos ~len ->
                let pos_ref = ref pos in
                let response = String.bin_read_t buf ~pos_ref in
                assert (!pos_ref - pos = len);
                Ivar.fill i (Ok response);
                Deferred.unit
              )
            : [`Connection_closed | `Flushed of unit Deferred.t]
          ))
        >>| fun response ->
        Log.debug log "got response";
        <:test_result< string Or_error.t >> response ~expect:(Ok the_response)
      )
      >>| fun result ->
      Result.ok_exn result
    )
    >>= fun () ->
    Tcp.Server.close server

  let command =
    Command.async ~summary:"connect basic and low-level clients"
      Command.Spec.(
        empty
        +> flag "debug" no_arg ~doc:""
      )
      main
end

let () =
  Command.run
    (Command.group ~summary:"Various tests for rpcs"
       [ "rpc",
         Command.group ~summary:"Plain rpc performance test"
           [ "server", Rpc_performance_measurements.server_command
           ; "client", Rpc_performance_measurements.client_command
           ];
         "pipe",
         (Command.group ~summary:"Pipe rpc"
            [ "simple", Pipe_simple_test.command;
              "performance", Pipe_rpc_performance_measurements.command
            ]);
         "expert",
         (Command.group ~summary:"Testing Expert interface"
            [ "test", Rpc_expert_test.command;
            ]);
       ])
