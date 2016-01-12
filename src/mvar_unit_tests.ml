open! Core.Std
open! Import

let async_unit_test f =
  Thread_safe.block_on_async_exn (fun () ->
    Clock.with_timeout (sec 10.) (f ())
    >>= function
    | `Result () -> return ()
    | `Timeout   -> failwith "test took too long")
;;

let%test_module _ =
  (module (struct
    open Mvar

    type nonrec ('a, 'phantom) t = ('a, 'phantom) t [@@deriving sexp_of]

    let%test_unit "sexp_of_t empty, with a pointer ['a] does not segfault" =
      let t = create () in
      ignore (t |> [%sexp_of: (string, _) t] |> Sexp.to_string)
    ;;

    let%test_unit "sexp_of_t non_empty" =
      let t = create () in
      set t 13;
      ignore (t |> [%sexp_of: (int, _) t] : Sexp.t)
    ;;

    module Read_only = struct
      open Read_only

      type nonrec 'a t = 'a t [@@deriving sexp_of]

      let invariant = invariant
    end

    module Read_write = struct
      open Read_write

      type nonrec 'a t = 'a t [@@deriving sexp_of]

      let invariant = invariant
    end

    let create          = create
    let is_empty        = is_empty
    let is_empty        = is_empty
    let put             = put
    let read_only       = read_only
    let set             = set
    let peek            = peek
    let peek_exn        = peek_exn
    let take            = take
    let take_exn        = take_exn
    let taken           = taken
    let value_available = value_available

    let%test_unit "is_empty" =
      let t = create () in
      assert (is_empty t);
      set t 13;
      assert (not (is_empty t));
    ;;

    let%test_unit "peek, peek_exn" =
      let t = create () in
      [%test_result: int option] (peek t) ~expect:None;
      assert (does_raise (fun () -> peek_exn t));
      set t 13;
      [%test_result: int option] (peek t) ~expect:(Some 13);
      [%test_result: int] (peek_exn t) ~expect:13;
    ;;

    let%test_unit "value_available works" =
      async_unit_test (fun () ->
        let t = create () in
        set t 7;
        Clock.with_timeout (sec 1.) (value_available (read_only t))
        >>= function
        | `Timeout   -> failwith "value_available not filled correctly"
        | `Result () -> Deferred.unit)
    ;;

    let%test_unit "value_available returns the same deferred if value not taken" =
      let t = create () in
      let rt = read_only t in
      let v1 = value_available rt in
      let v2 = value_available rt in
      assert (take rt = None);
      let v3 = value_available rt in
      assert (phys_equal v1 v2);
      assert (phys_equal v1 v3);
    ;;

    let%test_unit "value_available returns a fresh deferred with an intervening take" =
      let t = create () in
      let rt = read_only t in
      let v1 = value_available rt in
      set t ();
      take_exn rt;
      let v2 = value_available rt in
      assert (not (phys_equal v1 v2));
    ;;

    let%test_unit "taken works" =
      async_unit_test (fun () ->
        let t = create () in
        let taken = taken t in
        set t ();
        take_exn (read_only t);
        Clock.with_timeout (sec 1.) taken
        >>= function
        | `Timeout   -> failwith "taken not filled correctly"
        | `Result () -> Deferred.unit)
    ;;

    let%test_unit "set overwrites previous values" =
      let t = create () in
      set t 1;
      set t 2;
      assert (take (read_only t) = Some 2);
    ;;

    let%test_unit "put waits" =
      async_unit_test (fun () ->
        let t = create () in
        let rt = read_only t in
        set t 1;
        let put = put t 2 in
        assert (take_exn rt = 1);
        put
        >>= fun () ->
        [%test_result: int] (take_exn rt) ~expect:2;
        return ())
    ;;

    let%test_unit "two puts wait twice" =
      async_unit_test (fun () ->
        let t = create () in
        let rt = read_only t in
        set t 1;
        let put1 = put t 1 in
        let put2 = put t 2 in
        value_available rt
        >>= fun () ->
        ignore (take_exn rt : int);
        value_available rt
        >>= fun () ->
        ignore (take_exn rt : int);
        put1
        >>= fun () ->
        put2
        >>= fun () ->
        return ())
    ;;
  end
           (* This signature constraint is here to remind us to add a unit test whenever the
              interface to [Mvar] changes. *)
           : module type of Mvar))

