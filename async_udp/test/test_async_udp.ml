open! Core
open Poly
open! Async
open! Import
open! Async_udp

module Ready_iter = struct
  open Private.Ready_iter

  module Ok = struct
    open Ok

    let%test_unit _ =
      List.iter all ~f:(fun t ->
        let i = to_int t in
        [%test_result: t] ~expect:t (of_int_exn i);
        [%test_result: int] ~expect:i (to_int t);
        List.iter all ~f:(fun u ->
          let j = to_int u in
          if Bool.( <> ) (compare u t = 0) (j = i)
          then
            failwiths
              "overlapping representations"
              (t, i, u, j)
              [%sexp_of: t * int * t * int]))
    ;;
  end
end
