module Async_limiter = Limiter
open Core.Std
open Import
module Limiter = Async_limiter

let async_unit_test = Thread_safe.block_on_async_exn
let stabilize = Async_kernel.Scheduler.run_cycles_until_no_jobs_remain

TEST_MODULE = (struct
  open Limiter

  module Outcome = Outcome

  type t = Limiter.t with sexp_of
  type limiter = t with sexp_of

  module type Common = Common

  let fill_if_zero r i =
    decr r;
    if !r <= 0
    then Ivar.fill_if_empty i ()
  ;;

  module Token_bucket = struct
    open Token_bucket

    type t = Token_bucket.t
    type _ u = t

    let create_exn         = create_exn
    let enqueue_exn        = enqueue_exn
    let enqueue'           = enqueue'
    let kill               = kill
    let is_dead            = is_dead
    let to_limiter         = to_limiter

    TEST_UNIT "rate limit is honored" =
      async_unit_test (fun () ->
        let rate_per_second = 1000. in
        let t =
          create_exn ~burst_size:1. ~sustained_rate_per_sec:rate_per_second
            ~continue_on_error:false ()
        in
        let job_count          = rate_per_second /. 2. in
        let start_time         = Scheduler.cycle_start () in
        let min_time           = Time.add start_time (sec 0.5) in
        let jobs_remaining     = ref (Float.to_int job_count) in
        let finished           = Ivar.create () in
        for _i = 1 to !jobs_remaining do
          enqueue_exn t 1. (fun () -> fill_if_zero jobs_remaining finished) ();
        done;
        Ivar.read finished
        >>| fun () ->
        assert (Time.(>=) (Scheduler.cycle_start ()) min_time))
    ;;

    TEST_UNIT "burst rate is honored" =
      async_unit_test (fun () ->
        let rate_per_second = 1000. in
        let burst_size = 100 in
        let t =
          let burst_size = Float.of_int burst_size in
          create_exn
            ~burst_size:burst_size
            ~sustained_rate_per_sec:rate_per_second
            ~continue_on_error:false
            ~initial_burst_size:burst_size
            ()
        in
        let job_count         = ref (burst_size * 2) in
        let hit_burst_rate    = ref false in
        let current_job_count = ref 0 in
        let finished          = Ivar.create () in
        for _i = 1 to !job_count do
          enqueue' t 1. (fun () ->
            incr current_job_count;
            if !current_job_count = burst_size
            then hit_burst_rate := true;
            assert (!current_job_count <= burst_size);
            Deferred.unit
            >>| fun () ->
            decr current_job_count;
            fill_if_zero job_count finished) ()
          >>> (function
            | Ok () -> ()
            | Aborted | Raised _ -> assert false)
        done;
        Ivar.read finished
        >>| fun () ->
        assert !hit_burst_rate);
    ;;

    TEST_UNIT "allow_immediate_run is honored" =
      let t =
        create_exn ~burst_size:1. ~sustained_rate_per_sec:(1. /. 100.)
          ~continue_on_error:false ~initial_burst_size:1. ()
      in
      let num_jobs_run = ref 0 in
      let job () = incr num_jobs_run in
      enqueue_exn t ~allow_immediate_run:true 1. job ();
      enqueue_exn t ~allow_immediate_run:true 1. job ();
      assert (!num_jobs_run = 1)
    ;;
  end

  module Throttle = struct
    open Throttle

    type t = Throttle.t
    type _ u = t

    let create_exn                     = create_exn
    let enqueue_exn                    = enqueue_exn
    let enqueue'                       = enqueue'
    let concurrent_jobs_target         = concurrent_jobs_target
    let set_concurrent_jobs_target_exn = set_concurrent_jobs_target_exn
    let num_jobs_running               = num_jobs_running
    let num_jobs_waiting_to_start      = num_jobs_waiting_to_start
    let kill                           = kill
    let is_dead                        = is_dead
    let to_limiter                     = to_limiter

    let assert_concurrent_jobs_target_honored (t : t) concurrent_jobs_target =
      let max_running_concurrently = ref 0 in
      let num_running              = ref 0 in
      let job_count                = ref (concurrent_jobs_target * 2) in
      let finished                 = Ivar.create () in
      for _i = 1 to !job_count do
        enqueue' t (fun () ->
          incr num_running;
          max_running_concurrently := Int.max !max_running_concurrently !num_running;
          if !num_running > concurrent_jobs_target
          then failwithf  "number of running jobs (%i) is greater than \
                           concurrent_jobs_target (%i)\n%!"
                 !num_running concurrent_jobs_target ();
          Deferred.unit
          >>| fun () ->
          decr num_running) ()
        >>> (function
          | Ok ()    -> fill_if_zero job_count finished
          | Aborted  -> assert false
          | Raised e -> raise e)
      done;
      Ivar.read finished
      >>| fun () ->
      if !max_running_concurrently < concurrent_jobs_target
      then failwithf "concurrent_jobs_target was set to %i, but we only ever ran %i at once"
             concurrent_jobs_target !max_running_concurrently ()
    ;;

    TEST_UNIT "concurrent_jobs_target is honored" =
      async_unit_test (fun () ->
        let concurrent_jobs_target = 100 in
        let t = create_exn ~concurrent_jobs_target ~continue_on_error:false () in
        assert_concurrent_jobs_target_honored t concurrent_jobs_target)
    ;;

    TEST_UNIT "set_concurrent_jobs_target works" =
      let target_settings = [ 5; 10; 15; 20; 5; 20; 15; 10 ] in
      async_unit_test (fun () ->
        let t = create_exn ~concurrent_jobs_target:10 ~continue_on_error:false () in
        Deferred.List.iter target_settings ~f:(fun concurrent_jobs_target ->
          set_concurrent_jobs_target_exn t concurrent_jobs_target;
          assert_concurrent_jobs_target_honored t concurrent_jobs_target))
    ;;

    (* tests from the previous Throttle implementation *)
    TEST =
      try
        ignore (create_exn ~continue_on_error:false ~concurrent_jobs_target:0 ());
        false
      with _ -> true
    ;;

    TEST_UNIT "enqueue does not start the job immediately" =
      let t = create_exn ~continue_on_error:false ~concurrent_jobs_target:1 () in
      let i = ref 0 in
      let (_ : unit Outcome.t Deferred.t) = enqueue' t (fun () -> incr i; Deferred.unit) () in
      assert (!i = 0);
      stabilize ();
      assert (!i = 1)
    ;;

    TEST_UNIT "jobs are started in the order they are enqueued" =
      async_unit_test (fun () ->
        let t = create_exn ~continue_on_error:false ~concurrent_jobs_target:2 () in
        assert (concurrent_jobs_target t = 2);
        let r = ref [] in
        let job_count = ref 99 in
        let finished  = Ivar.create () in
        for i = 0 to !job_count do
          don't_wait_for (
            enqueue' t (fun () -> r := i :: !r; Deferred.unit) ()
            >>| fun (_ : unit Outcome.t) ->
            fill_if_zero job_count finished);
        done;
        Ivar.read finished
        >>| fun () ->
        assert (!r = List.rev (List.init 100 ~f:Fn.id)));
    ;;

    TEST_UNIT "jobs waiting to start and jobs running are sane" =
      let t = create_exn ~continue_on_error:false ~concurrent_jobs_target:2 () in
      assert (num_jobs_waiting_to_start t = 0);
      let add_job () =
        let ivar = Ivar.create () in
        don't_wait_for (
          enqueue' t (fun () -> Ivar.read ivar) ()
          >>| fun (_ : unit Outcome.t) -> ());
        ivar;
      in
      let i1 = add_job () in
      assert (num_jobs_waiting_to_start t + num_jobs_running t = 1);
      stabilize ();
      assert (num_jobs_waiting_to_start t = 0);
      assert (num_jobs_running t = 1);
      let _i2 = add_job () in
      assert (num_jobs_waiting_to_start t + num_jobs_running t = 2);
      stabilize ();
      assert (num_jobs_waiting_to_start t = 0);
      assert (num_jobs_running t = 2);
      let _i3 = add_job () in
      assert (num_jobs_waiting_to_start t = 1);
      assert (num_jobs_running t = 2);
      stabilize ();
      assert (num_jobs_waiting_to_start t = 1);
      assert (num_jobs_running t = 2);
      Ivar.fill i1 ();
      stabilize ();
      assert (num_jobs_waiting_to_start t = 0);
      assert (num_jobs_running t = 2);
    ;;

    TEST_UNIT "jobs enqueued in the same cycle as kill are aborted" =
      let t = create_exn ~continue_on_error:false ~concurrent_jobs_target:1 () in
      let r = ref false in
      let d = enqueue' t (fun () -> r := true; return ()) () in
      kill t;
      stabilize ();
      assert (Deferred.peek d = Some Aborted);
      assert (not !r);
    ;;

    TEST_UNIT "jobs enqueued after kill are aborted" =
      let t = create_exn ~continue_on_error:false ~concurrent_jobs_target:1 () in
      kill t;
      let r = ref true in
      let d = enqueue' t (fun () -> r := false; return ()) () in
      stabilize ();
      assert (Deferred.peek d = Some Aborted);
      assert !r;
    ;;

    TEST_UNIT "enqueueing withing a job doesn't lead to monitor nesting" =
      let seq = create_exn ~concurrent_jobs_target:1 ~continue_on_error:false () in
      let rec loop n =
        if n = 0
        then Deferred.unit
        else
          enqueue' seq (fun () ->
            assert (Monitor.depth (Monitor.current ()) < 5);
            don't_wait_for (loop (n - 1));
            Deferred.unit) ()
          >>| fun (_ : unit Outcome.t) -> ()
      in
      let d = loop 100 in
      stabilize ();
      assert (Deferred.peek d = Some ());
    ;;
  end

  module Resource_throttle = struct
    open Resource_throttle

    type 'a t = 'a Resource_throttle.t

    let create_exn          = create_exn
    let enqueue_exn         = enqueue_exn
    let enqueue'            = enqueue'
    let to_limiter          = to_limiter
    let max_concurrent_jobs = max_concurrent_jobs
    let kill                = kill
    let is_dead             = is_dead
    let to_limiter          = to_limiter

    module Resource = struct
      type t = int ref

      let use (t : t) =
        incr t;
        assert (!t = 1);
      ;;

      let release (t : t) = decr t

      let create () = ref 0
    end

    TEST_UNIT "resources are never double used" =
      async_unit_test (fun () ->
        let resources = [ Resource.create (); Resource.create (); Resource.create () ] in
        let t =
          create_exn
            ~resources
            ~continue_on_error:false
            ()
        in
        let job_count = ref 100 in
        let finished  = Ivar.create () in
        for _i = 1 to !job_count do
          enqueue' t
            (fun r ->
               Resource.use r;
               Deferred.unit
               >>| fun () ->
               Resource.release r)
          >>> (function
            | Ok () -> fill_if_zero job_count finished
            | Aborted | Raised _ -> assert false);
        done;
        Ivar.read finished
        >>| fun () ->
        List.iter resources ~f:(fun r -> assert (!r = 0)));
  end

  module Sequencer = struct
    type t = Sequencer.t
    type _ u = t
    open Sequencer

    let create             = create
    let enqueue_exn        = enqueue_exn
    let enqueue'           = enqueue'
    let kill               = kill
    let is_dead            = is_dead
    let to_limiter         = to_limiter

  end

  TEST_UNIT "sequencers run only one job at a time" =
    async_unit_test (fun () ->
      let t             = Sequencer.create ~continue_on_error:true () in
      let num_jobs_run  = ref 0 in
      let expected_jobs = 100 in
      let job_count     = ref expected_jobs in
      let finished      = Ivar.create () in
      for _i = 1 to !job_count do
        Sequencer.enqueue' t (fun () -> incr num_jobs_run; assert false) ()
        >>> (function
          | Ok _ | Aborted -> assert false
          | Raised _ -> fill_if_zero job_count finished)
      done;
      Ivar.read finished
      >>| fun () ->
      assert (!num_jobs_run = expected_jobs));
  ;;

  TEST_UNIT "jobs can kill the throttle" =
    async_unit_test (fun () ->
      let t = Sequencer.create () in
      let num_ok = ref 0 in
      let num_aborted = ref 0 in
      let jobs_remaining = ref 100 in
      let all_jobs_returned = Ivar.create () in
      let num_jobs_run = ref 0 in
      for _i = 1 to !jobs_remaining do
        Sequencer.enqueue' t (fun () ->
          incr num_jobs_run;
          if !num_jobs_run = 1
          then Sequencer.kill t;
          Deferred.unit) ()
        >>> (fun res ->
          decr jobs_remaining;
          if !jobs_remaining = 0
          then Ivar.fill all_jobs_returned ();
          match res with
          | Ok () -> incr num_ok
          | Aborted -> incr num_aborted
          | Raised _ -> assert false)
      done;
      Ivar.read all_jobs_returned
      >>| fun () ->
      assert (!num_ok = 1);
      assert (!num_aborted = 99));
  ;;

  module Expert = struct
    open Expert
    let cost_of_jobs_waiting_to_start = cost_of_jobs_waiting_to_start
    let to_core_limiter               = to_core_limiter
    let is_dead                       = is_dead
    let kill                          = kill
  end
end
(* This signature constraint is here to remind us to add a unit test whenever the
   interface to [Bus] changes. *)
: module type of Limiter)
