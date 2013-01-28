
open Core.Std

module Make (M : sig
  val add_finalizer : 'a Heap_block.t -> ('a Heap_block.t -> unit) -> unit
end) = struct
  module Entry_id : Unique_id.Id = Unique_id.Int63 (struct end)

  module Entry = struct
    type 'a t =
      { mutable id : Entry_id.t
      ; data : 'a Weak.t
      }
    with sexp_of

    let create () = { id = Entry_id.create (); data = Weak.create ~len:1 }

    let data t = Weak.get t.data 0
  end

  type ('a, 'b) t = ('a, 'b Entry.t) Hashtbl.t with sexp_of

  let create hashable = Hashtbl.create ~hashable ()

  let maybe_remove t key entry_id =
    Option.iter (Hashtbl.find t key) ~f:(fun entry ->
      if Entry_id.(=) entry.Entry.id entry_id
      then Hashtbl.remove t key)
  ;;

  let get_entry t key = Hashtbl.find_or_add t key ~default:Entry.create

  let set_data t key entry data =
    let entry_id = Entry_id.create () in
    entry.Entry.id <- entry_id;
    Weak.set entry.Entry.data 0 (Some data);
    M.add_finalizer data (fun _ -> maybe_remove t key entry_id);
  ;;

  let replace t ~key ~data = set_data t key (get_entry t key) data

  let find t key =
    let open Option.Monad_infix in
    Hashtbl.find t key
    >>= fun entry ->
    Entry.data entry
  ;;

  let find_or_add t key ~default =
    let entry = get_entry t key in
    match Entry.data entry with
    | Some v -> v
    | None ->
      let data = default () in
      set_data t key entry data;
      data
  ;;

  let remove = Hashtbl.remove
end

TEST_UNIT =
  let module T = Make (struct
    (* We use [Caml.Gc.finalise] rather than [Core.Gc.add_finalizer] so that finalizers
       run immediately after calling [full_major], in our thread.  If we used
       [Core.Gc.add_finalizer], the finalizers would get queued to run in another thread,
       and we would have to wait for that thread to run the finalizers before checking
       that the properties that we desire from their running hold. *)
    let add_finalizer x f = Caml.Gc.finalise f x
  end)
  in
  let open T in
  let module M = struct
    type t =
      { foo : int
      ; bar : int
      ; baz : string
      }
  end
  in
  let open M in
  let block foo = Heap_block.create_exn ({ foo; bar = 0; baz = "hello" }, 0) in
  let tbl = create Int.hashable in
  let add k b = ignore (find_or_add tbl k ~default:(fun () -> !b)) in
  (* We put the blocks in refs and manually blackhole them, so that the unit test will
     pass with the bytecode compiler. *)
  let b1 = ref (block 1) in
  let b2 = ref (block 2) in
  let b3 = ref (block 3) in
  let b4 = ref (block 4) in
  let blackhole b = b := block 0 in
  let k1 = 1 in
  let k2 = 2 in
  let k3 = 3 in
  add k1 b1;
  add k2 b2;
  add k3 b3;
  (* Checking [is_absent k] is stronger than checking that [is_none (find tbl k)].  We
     want to make sure that a key has been removed from the table, and in particular rule
     out the case where the key is in the table but the corresponding weak is none. *)
  let is_absent k = not (Hashtbl.mem tbl k) in
  let is_block k b =
    match find tbl k with
    | None -> false
    | Some v -> phys_equal v b
  in
  assert (is_block k1 !b1);
  assert (is_block k2 !b2);
  assert (is_block k3 !b3);
  blackhole b1;
  Gc.full_major ();
  assert (is_absent k1);
  assert (is_block k2 !b2);
  assert (is_block k3 !b3);
  blackhole b2;
  Gc.full_major ();
  assert (is_absent k1);
  assert (is_absent k2);
  assert (is_block k3 !b3);
  replace tbl ~key:k3 ~data:!b4;
  blackhole b3;
  Gc.full_major ();
  assert (is_block k3 !b4);
  blackhole b4;
  Gc.full_major ();
  assert (is_absent k3);
;;

include Make (Async_unix.Async_gc)
