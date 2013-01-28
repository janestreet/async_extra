
(* A [Weak_hashtbl.t] is a hashtable that will drop a key and value if the value is no
   longer referenced (by any non-weak pointers).  [Weak_hashtbl] is in Async rather than
   Core because it relies on finalization in its implementation.  Using non-Async
   finalizers in an async program is wrong (see Async_gc.mli for details), and having
   [Weak_hashtbl] in Async prevents users from making this mistake.  Unlike (OCaml's)
   [Weak.Make], which also describes itself as a "weak hashtable," [Weak_hashtbl] gives a
   dictionary style structure.  In fact, OCaml's Weak.Make may better be described as a
   weak set.

   There's a tricky type of bug one can write with this module, e.g.:

   | type t =
   |   { foo : string
   |   ; bar : float Incr.t
   |   }
   |
   | let tbl = Weak_hashtbl.create ()
   | let x1 =
   |   let t = Weak_hashtbl.find_or_add tbl key ~default:(fun () ->
   |     (... some function that computes a t...))
   |   in
   |   t.bar

   At this point, the value associated with [key] is unreachable (since all we did with it
   was project out field bar), so it may disappear from the table at any time. *)

open Core.Std

type ('a, 'b) t

val create : 'a Hashtbl.Hashable.t -> ('a, 'b) t

val find        : ('a, 'b) t -> 'a -> 'b Heap_block.t option
val find_or_add : ('a, 'b) t -> 'a -> default:(unit -> 'b Heap_block.t) -> 'b Heap_block.t
val remove      : ('a, 'b) t -> 'a -> unit
val replace     : ('a, 'b) t -> key:'a -> data:'b Heap_block.t -> unit

