open Core.Std
open Import

module Make (Key : Hashable) = struct
  type 'a t =
    { states : 'a Key.Table.t
    (* We use a [Queue.t] and implement the [Throttle.Sequencer] functionality ourselves,
       because throttles don't provide a way to get notified when they are empty, and we
       need to remove the table entry for an emptied throttle. *)
    ; jobs : ('a option -> unit Deferred.t) Queue.t Key.Table.t
    } with fields

  let create () =
    { states = Key.Table.create ()
    ; jobs   = Key.Table.create ()
    }
  ;;

  let rec run_jobs_until_none_remain t ~key queue =
    match Queue.peek queue with
    | None -> Hashtbl.remove t.jobs key
    | Some job ->
      job (Hashtbl.find t.states key)
      >>> fun () ->
      assert (phys_equal (Queue.dequeue_exn queue) job);
      run_jobs_until_none_remain t ~key queue;
  ;;

  let set_state t ~key = function
    | None       -> Hashtbl.remove  t.states  key
    | Some state -> Hashtbl.replace t.states ~key ~data:state
  ;;

  let enqueue t ~key f =
    Deferred.create (fun ivar ->
      let job state_opt =
        Monitor.try_with (fun () -> f state_opt) >>| Ivar.fill ivar
      in
      match Hashtbl.find t.jobs key with
      | Some queue ->
        Queue.enqueue queue job
      | None ->
        let queue = Queue.create () in
        Queue.enqueue queue job;
        Hashtbl.replace t.jobs ~key ~data:queue;
        (* never start a job in the same async job *)
        upon Deferred.unit (fun () ->
          run_jobs_until_none_remain t ~key queue);
    )
    >>| function
    | Error exn -> raise (Monitor.extract_exn exn)
    | Ok res -> res
  ;;

  let find_state t key = Hashtbl.find t.states key

  let num_unfinished_jobs t key =
    match Hashtbl.find t.jobs key with
    | None -> 0
    | Some queue -> Queue.length queue
  ;;

  let mem t key = Hashtbl.mem t.states key || Hashtbl.mem t.jobs key

  let fold t ~init ~f =
    let all_keys =
      Key.Hash_set.create ~size:(Hashtbl.length t.jobs + Hashtbl.length t.states) ()
    in
    Hashtbl.iter t.jobs   ~f:(fun ~key ~data:_ -> Hash_set.add all_keys key);
    Hashtbl.iter t.states ~f:(fun ~key ~data:_ -> Hash_set.add all_keys key);
    Hash_set.fold all_keys ~init ~f:(fun acc key ->
      f acc ~key (Hashtbl.find t.states key))
  ;;

end

TEST_MODULE = struct
  module T = Make(Int)

  let (=) = Pervasives.(=)

  exception Abort of int


  TEST_UNIT =
    Thread_safe.block_on_async_exn (fun () ->
      let t = T.create () in
      let i = ref 0 in
      let res = T.enqueue t ~key:0 (fun _ -> incr i; Deferred.unit) in
      (* don't run a job immediately *)
      assert (!i = 0);
      res >>| fun () ->
      assert (!i = 1)
    )

  TEST_UNIT =
    Thread_safe.block_on_async_exn (fun () ->
      let t = T.create () in
      let num_keys = 100 in
      let keys = List.init num_keys ~f:Fn.id in
      let started_jobs = Queue.create () in
      let enqueue key x =
        Monitor.try_with (fun () ->
          T.enqueue t ~key (fun state ->
            let state =
              match state with
              | None -> [x]
              | Some xs -> x::xs
            in
            T.set_state t ~key (Some state);
            Queue.enqueue started_jobs key;
            Clock.after (sec 0.01) >>| fun () ->
            (* check continue on error *)
            raise (Abort key)
          )
        )
        >>| function
        | Error exn ->
          begin match Monitor.extract_exn exn with
          | (Abort i) when i = key -> ()
          | _ -> assert false
          end
        | _ -> assert false
      in
      Deferred.List.iter keys ~how:`Parallel ~f:(fun key ->
        Deferred.List.iter ['a'; 'b'; 'c' ] ~how:`Parallel ~f:(enqueue key)
      )
      >>| fun () ->
      List.iter keys ~f:(fun key ->
        (* check [find_state] *)
        (* check jobs are sequentialized for the same key *)
        assert (T.find_state t key = Some ['c'; 'b'; 'a'])
      );
      (* check jobs on different keys can run concurrently *)
      let started_jobs_in_batched =
        List.groupi (Queue.to_list started_jobs)
          ~break:(fun i _ _ -> i mod num_keys = 0)
      in
      List.iter started_jobs_in_batched ~f:(fun l ->
        assert (List.sort l ~cmp:Int.compare = keys)
      );
    )
  ;;

  TEST_UNIT =
    (* Test [num_unfinished_jobs] *)
    Thread_safe.block_on_async_exn (fun () ->
      let t = T.create () in
      assert (T.num_unfinished_jobs t 0 = 0);
      let job1 =
        T.enqueue t ~key:0 (fun _ ->
          assert (T.num_unfinished_jobs t 0 = 3); Deferred.unit)
      in
      let job2 =
        T.enqueue t ~key:0 (fun _ ->
          assert (T.num_unfinished_jobs t 0 = 2); Deferred.unit)
      in
      let job3 =
        T.enqueue t ~key:0 (fun _ ->
          assert (T.num_unfinished_jobs t 0 = 1); Deferred.unit)
      in
      assert (T.num_unfinished_jobs t 0 = 3);
      Deferred.all_unit [job1; job2; job3] >>| fun () ->
      assert (T.num_unfinished_jobs t 0 = 0)
    )

  TEST_UNIT =
    (* Test [mem] *)
    Thread_safe.block_on_async_exn (fun () ->
      let t = T.create () in
      (* empty *)
      assert (T.mem t 0 = false);
      let job = T.enqueue t ~key:0 (fun _ -> Deferred.unit) in
      (* with job *)
      assert (T.mem t 0);
      job >>= fun () ->
      (* without job *)
      assert (T.mem t 0 = false);
      (* with state *)
      T.set_state t ~key:0 (Some 'a');
      assert (T.mem t 0);
      T.set_state t ~key:0 None;
      (* without state *)
      assert (T.mem t 0 = false);
      let job =
        T.set_state t ~key:0 (Some 'a');
        T.enqueue t ~key:0 (fun _ -> Deferred.unit)
      in
      (* with job and state *)
      assert (T.mem t 0);
      job >>| fun () ->
      (* without job but with state *)
      assert (T.mem t 0)
    )
end
