
open Core.Std

module Box : sig
  type 'a t with sexp_of
  val inject  : 'a -> 'a t
  val project : 'a t -> 'a
  val touch : _ t -> unit
end = struct
  type 'a t = { value : 'a }
  let sexp_of_t sexp_of_a t = sexp_of_a t.value
  let project t = t.value
  let inject value = { value }
  let touch _ = ()
end

module Weak = struct
  include Weak

  let to_array t = Array.init (length t) ~f:(fun i -> get t i)

  let sexp_of_t sexp_of_a t = <:sexp_of< a option array >> (to_array t)
end

module Make (M : sig
  val add_finalizer : ('a -> unit) -> 'a -> unit
end) = struct
  module Entry_id : Unique_id.Id = Unique_id.Int63 (struct end)

  module Entry = struct
    type 'a t =
      { mutable id : Entry_id.t
      ; data : 'a Box.t Weak.t
      }
    with sexp_of
  end

  type ('a, 'b) t = ('a, 'b Entry.t) Hashtbl.t with sexp_of

  let create hashable = Hashtbl.create ~hashable ()

  let maybe_remove t key entry_id =
    Option.iter (Hashtbl.find t key) ~f:(fun entry ->
      if Entry_id.(=) entry.Entry.id entry_id
      then Hashtbl.remove t key)
  ;;

  let replace t ~key ~data =
    let entry_id = Entry_id.create () in
    let entry = Hashtbl.find_or_add t key ~default:(fun () ->
      { Entry.id = entry_id; data = Weak.create 1 })
    in
    entry.Entry.id <- entry_id;
    Weak.set entry.Entry.data 0 (Some data);
    M.add_finalizer (fun _ -> maybe_remove t key entry_id) data;
  ;;

  let find t key =
    let open Option.Monad_infix in
    Hashtbl.find t key
    >>= fun entry ->
    Weak.get entry.Entry.data 0
    >>| fun data ->
    Box.project data
  ;;

  let find_or_add t key ~default =
    match find t key with
    | Some value -> value
    | None ->
      let data = default () in
      replace t ~key ~data;
      Box.project data
  ;;

  let remove = Hashtbl.remove
end

TEST_UNIT =
  let module T = Make (struct
    (* We use [Caml.Gc.finalise] rather than [Core.Gc.finalise] so that finalizers
       run immediately after calling [full_major], in our thread.  If we used
       [Core.Gc.finalise], the finalizers would get queued to run in another thread,
       and we would have to wait for that thread to run the finalizers before checking
       that the properties that we desire from their running hold. *)
    let add_finalizer = Caml.Gc.finalise
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
  let box foo = Box.inject ({ foo; bar = 0; baz = "hello" }, 0) in
  let b1 = box 1 in
  let b2 = box 2 in
  let b3 = box 3 in
  let b4 = box 4 in
  let tbl = create Int.hashable in
  let k1 = 1 in
  let k2 = 2 in
  let k3 = 3 in
  let (_ : M.t * int) = find_or_add tbl k1 ~default:(fun () -> b1) in
  let (_ : M.t * int) = find_or_add tbl k2 ~default:(fun () -> b2) in
  let (_ : M.t * int) = find_or_add tbl k3 ~default:(fun () -> b3) in
  (* Checking [is_absent k] is stronger than checking that [is_none (find tbl k)].  We
     want to make sure that a key has been removed from the table, and in particular rule
     out the case where the key is in the table but the corresponding weak is none.
  *)
  let is_absent k = not (Hashtbl.mem tbl k) in
  let is_box k b =
    match find tbl k with
    | None -> false
    | Some v -> phys_equal v (Box.project b)
  in
  assert (is_box k1 b1);
  assert (is_box k2 b2);
  assert (is_box k3 b3);
  Gc.full_major ();
  assert (is_absent k1);
  assert (is_box k2 b2);
  assert (is_box k3 b3);
  Box.touch b2;
  Gc.full_major ();
  assert (is_absent k1);
  assert (is_absent k2);
  assert (is_box k3 b3);
  replace tbl ~key:k3 ~data:b4;
  Box.touch b3;
  Gc.full_major ();
  assert (is_box k3 b4);
  Box.touch b4;
  Gc.full_major ();
  assert (is_absent k3);
;;

include Make (Async_unix.Async_gc)
