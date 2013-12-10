open Core.Std
open Import
open Int.Replace_polymorphic_compare

type write_buffer = (read_write, Iobuf.seek) Iobuf.t

let default_capacity = 1472

module Config = struct
  type t =
    { capacity : int
    ; init : write_buffer
    ; before : write_buffer -> unit
    ; after : write_buffer -> unit
    ; stop : unit Deferred.t
    } with fields

  let create
        ?(capacity = default_capacity)
        ?(init = Iobuf.create ~len:capacity)
        ?(before = Iobuf.flip_lo)
        ?(after = Iobuf.reset)
        ?(stop = Deferred.never ())
        ()
    =
    { capacity; init; before; after; stop }
end

let fail iobuf message a sexp_of_a =
  (* Render buffers immediately, before we have a chance to change them. *)
  failwiths message (a, <:sexp_of< (_, _) Iobuf.t >> iobuf)
    (Tuple.T2.sexp_of_t sexp_of_a ident)
;;

let failf iobuf =
  ksprintf (fun message ->
    failwiths message (<:sexp_of< (_, _) Iobuf.t >> iobuf) ident)
;;

let sendto_sync () =
  Iobuf.sendto_nonblocking_no_sigpipe ()
  |> Or_error.map ~f:(fun sendto ->
    (fun fd buf addr ->
       Fd.with_file_descr_exn fd ~nonblocking:true (fun desc ->
         match sendto buf desc (Unix.Socket.Address.to_sockaddr addr) with
         | None -> `Not_ready
         | Some _ -> `Ok)))
;;

(** [ready_iter fd f] iterates [f] over [fd], handling [EWOULDBLOCK]/[EAGAIN] and [EINTR]
    by retrying when ready.  Iteration is terminated when [fd] closes, [stop] fills, or
    [f] returns [`Stop].

    [ready_iter] may fill [stop] itself.

    By design, this function might not return to the Async scheduler until [fd] is no
    longer ready to transfer data.  If you expect [fd] to be ready for a long period then
    you should use [stop] or [`Stop] to avoid starving other Async jobs. *)
let ready_iter fd ~stop ~f read_or_write =
  let rec inner_loop file_descr =
    if Ivar.is_empty stop && Fd.is_open fd then match f file_descr with
      | `Continue -> inner_loop file_descr
      | `Stop -> `Stopped
    else `Interrupted_or_closed
  in
  (* [Fd.with_file_descr] is for [Raw_fd.set_nonblock_if_necessary].
     [with_file_descr_deferred] would be the more natural choice, but it doesn't call
     [set_nonblock_if_necessary]. *)
  match Fd.with_file_descr ~nonblocking:true fd (fun file_descr ->
    Fd.interruptible_every_ready_to fd read_or_write ~interrupt:(Ivar.read stop)
      (fun file_descr ->
         try match inner_loop file_descr with
           | `Interrupted_or_closed -> ()
           | `Stopped -> Ivar.fill_if_empty stop ()
         with
         | Unix.Unix_error ((Unix.EAGAIN | Unix.EWOULDBLOCK | Unix.EINTR), _, _) -> ()
         | e -> Ivar.fill_if_empty stop (); raise e)
      file_descr)
  with
  | `Already_closed -> return `Closed
  | `Error e -> raise e
  (* Avoid one ivar creation by returning the result from
     [Fd.interruptible_every_ready_to] directly. *)
  | `Ok deferred -> deferred
;;

let sendto () =
  Iobuf.sendto_nonblocking_no_sigpipe ()
  |> Or_error.map ~f:(fun sendto -> unstage (stage (fun fd buf addr ->
    let addr = Unix.Socket.Address.to_sockaddr addr in
    let stop = Ivar.create () in
    (* Iobuf.sendto_nonblocking_no_sigpipe returns EAGAIN and EWOULDBLOCK through an
       option rather than an exception. *)
    ready_iter fd ~stop `Write ~f:(fun file_descr ->
      match sendto buf file_descr addr with None -> `Continue | Some _ -> `Stop)
    >>| function
    | (`Bad_fd | `Closed | `Unsupported) as error ->
      fail buf "Udp.sendto" (error, addr)
        <:sexp_of< [ `Bad_fd | `Closed | `Unsupported ] * Core.Std.Unix.sockaddr >>
    | `Interrupted -> ())))
;;

let bind ?ifname addr =
  let socket = Socket.create Socket.Type.udp in
  let is_multicast a =
    Unix.Cidr.does_match Unix.Cidr.multicast (Socket.Address.Inet.addr a)
  in
  if is_multicast addr
  then
    Core.Std.Unix.mcast_join ?ifname (Fd.file_descr_exn (Socket.fd socket))
      (Socket.Address.to_sockaddr addr);
  Socket.bind socket addr
;;

let bind_any () =
  let socket = Socket.create Socket.Type.udp in
  (* When bind() is called with a port number of zero, a non-conflicting local port
     address is chosen (i.e., an ephemeral port). *)
  let bind_addr = Socket.Address.Inet.create_bind_any ~port:0 in
  Socket.bind socket bind_addr
;;

let recvfrom_loop_with_buffer_replacement ?(config = Config.create ()) fd f =
  let stop = Ivar.create () in
  Config.stop config
  >>> Ivar.fill_if_empty stop;
  let buf = ref (Config.init config) in
  ready_iter ~stop fd `Read ~f:(fun file_descr ->
    let buf_len_before = Iobuf.length !buf in
    let len, addr = Iobuf.recvfrom_assume_fd_is_nonblocking !buf file_descr in
    if len <> buf_len_before - Iobuf.length !buf
    then
      failf !buf
        "Unexpected result from Iobuf.recvfrom_assume_fd_is_nonblocking: \
         len (%d) <> buf_len_before (%d) - Iobuf.length buf"
        len
        buf_len_before;
    match addr with
    | Core.Std.Unix.ADDR_UNIX dom ->
      fail !buf "Unix domain socket addresses not supported" dom <:sexp_of< string >>
    | Core.Std.Unix.ADDR_INET (host, port) ->
      Config.before config !buf;
      buf := f !buf (`Inet (host, port));
      Config.after  config !buf;
      `Continue)
  >>| function
  | (`Bad_fd | `Unsupported) as error ->
    fail !buf "Udp.recvfrom_loop_without_buffer_replacement" (error, fd)
      <:sexp_of< [ `Bad_fd | `Unsupported ] * Fd.t >>
  | `Closed | `Interrupted -> !buf
;;
let recvfrom_loop ?config fd f =
  recvfrom_loop_with_buffer_replacement ?config fd (fun b a -> f b a; b)
  >>| (ignore : (_, _) Iobuf.t -> unit)
;;

(* We don't care about the address, so read instead of recvfrom. *)
let read_loop_with_buffer_replacement ?(config = Config.create ()) fd f =
  let stop = Ivar.create () in
  Config.stop config
  >>> Ivar.fill_if_empty stop;
  let buf = ref (Config.init config) in
  ready_iter ~stop fd `Read ~f:(fun file_descr ->
    let buf_len_before = Iobuf.length !buf in
    let len = Iobuf.read_assume_fd_is_nonblocking !buf file_descr in
    if len <> buf_len_before - Iobuf.length !buf
    then
      failf !buf
        "Unexpected result from Iobuf.read_assume_fd_is_nonblocking: \
         len (%d) <> buf_len_before (%d) - Iobuf.length buf"
        len
        buf_len_before;
    Config.before config !buf;
    buf := f !buf;
    Config.after  config !buf;
    `Continue)
  >>| function
  | (`Bad_fd | `Unsupported) as error ->
    fail !buf "Udp.read_loop_with_buffer_replacement" (error, fd)
      <:sexp_of< [ `Bad_fd | `Unsupported ] * Fd.t >>
  | `Closed | `Interrupted -> !buf
;;
let read_loop ?config fd f =
  read_loop_with_buffer_replacement ?config fd (fun b -> f b; b)
  >>| (ignore : (_, _) Iobuf.t -> unit)
;;

let recvmmsg_loop =
  Or_error.map Iobuf.recvmmsg_assume_fd_is_nonblocking ~f:(fun recvmmsg ->
    (fun
      ?(config = Config.create ())
      ?(create_srcs = false)
      (* There's a 64kB threshold for the total buffer size before releasing the lock in
         ../../../core/lib/recvmmsg.c.  This is a good number to stay below. *)
      ?(max_count = 64 * 1024 / Config.capacity config)
      ?(bufs =
        Array.init max_count ~f:(function
          | 0 -> Config.init config
          | _ -> Iobuf.create ~len:(Iobuf.length (Config.init config))))
      fd
      f
      ->
        let srcs : Core.Std.Unix.sockaddr array option =
          if create_srcs
          then
            Some
              (Array.create ~len:(Array.length bufs)
                 (Socket.Address.to_sockaddr (`Inet (Unix.Inet_addr.bind_any, 0))))
          else None
        in
        let stop = Ivar.create () in
        Config.stop config
        >>> Ivar.fill_if_empty stop;
        ready_iter ~stop fd `Read ~f:(fun file_descr ->
          let count = recvmmsg ?srcs file_descr bufs in
          if count > Array.length bufs
          then
            failwithf
              "Unexpected result from Iobuf.recvmmsg_assume_fd_is_nonblocking: \
               count (%d) > Array.length bufs (%d)"
              count
              (Array.length bufs)
              ();
          for i = 0 to count - 1 do Config.before config bufs.(i) done;
          f ?srcs bufs ~count;
          for i = 0 to count - 1 do Config.after  config bufs.(i) done;
          `Continue)
        >>| function
        | (`Bad_fd | `Unsupported) as error ->
          failwiths "Udp.recvmmsg_loop" (error, fd)
            <:sexp_of< [ `Bad_fd | `Unsupported ] * Fd.t >>
        | `Closed | `Interrupted -> ()))
;;

let recvmmsg_no_sources_loop =
  Or_error.map Iobuf.recvmmsg_assume_fd_is_nonblocking_no_options ~f:(fun recvmmsg ->
    (fun
      ?(config = Config.create ())
      fd
      ?(max_count = 64 * 1024 / Config.capacity config)
      ?(bufs =
        Array.init max_count ~f:(function
          | 0 -> Config.init config
          | _ -> Iobuf.create ~len:(Iobuf.length (Config.init config))))
      callback
      ->
        let stop = Ivar.create () in
        Config.stop config
        >>> Ivar.fill_if_empty stop;
        ready_iter ~stop fd `Read ~f:(fun file_descr ->
          let count = recvmmsg file_descr ~count:max_count bufs in
          if count > Array.length bufs
          then
            failwithf
              "Unexpected result from \
               Iobuf.recvmmsg_assume_fd_is_nonblocking_no_options: \
               count (%d) > Array.length bufs (%d)"
              count
              (Array.length bufs)
              ();
          for i = 0 to count - 1 do Config.before config bufs.(i) done;
          callback bufs ~count;
          for i = 0 to count - 1 do Config.after  config bufs.(i) done;
          `Continue)
        >>| function
        | (`Bad_fd | `Unsupported) as error ->
          failwiths "Udp.recvmmsg_no_sources_loop" (error, fd)
            <:sexp_of< [ `Bad_fd | `Unsupported ] * Fd.t >>
        | `Closed | `Interrupted -> ()))
;;

let bind_to_interface_exn =
  Linux_ext.bind_to_interface
  |> Or_error.map ~f:(fun bind_to_interface ->
    (fun ~ifname fd ->
       Fd.with_file_descr_exn fd (fun file_descr ->
         bind_to_interface file_descr (`Interface_name ifname))))
;;
