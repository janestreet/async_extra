open Core.Std
open Import
open Int.Replace_polymorphic_compare

type write_buffer = (read_write, Iobuf.seek) Iobuf.t

let default_capacity = 1472

let default_retry = 12

module Config = struct
  type t =
    { capacity  : int
    ; init      : write_buffer
    ; before    : write_buffer -> unit
    ; after     : write_buffer -> unit
    ; stop      : unit Deferred.t
    ; max_ready : int
    } with fields

  let create
        ?(capacity = default_capacity)
        ?(init = Iobuf.create ~len:capacity)
        ?(before = Iobuf.flip_lo)
        ?(after = Iobuf.reset)
        ?(stop = Deferred.never ())
        ?(max_ready = default_retry)
        ()
    =
    { capacity; init; before; after; stop; max_ready }
end

let fail iobuf message a sexp_of_a =
  (* Render buffers immediately, before we have a chance to change them. *)
  failwiths message (a, <:sexp_of< (_, _) Iobuf.t >> iobuf)
    (Tuple.T2.sexp_of_t sexp_of_a ident)
;;

let sendto_sync () =
  Iobuf.sendto_nonblocking_no_sigpipe ()
  |> Or_error.map ~f:(fun sendto ->
    (fun fd buf addr ->
       Fd.with_file_descr_exn fd ~nonblocking:true (fun desc ->
         let len_before = Iobuf.length buf in
         sendto buf desc (Unix.Socket.Address.to_sockaddr addr);
         if len_before = Iobuf.length buf then `Not_ready else `Ok)))
;;

(** [ready_iter fd f] iterates [f] over [fd], handling [EWOULDBLOCK]/[EAGAIN] and [EINTR]
    by retrying when ready.  Iteration is terminated when [fd] closes, [stop] fills, or
    [f] returns [`Stop].

    [ready_iter] may fill [stop] itself.

    By design, this function will not return to the Async scheduler until [fd] is no
    longer ready to transfer data or has been ready [max_ready] consecutive times. To
    avoid starvation, use [stop] or [`Stop] and/or choose [max_ready] carefully to allow
    other Async jobs to run. *)
let ready_iter fd ~stop ~max_ready ~f read_or_write =
  let rec inner_loop i file_descr =
    if i < max_ready && Ivar.is_empty stop && Fd.is_open fd
    then
      match f file_descr with
      | `Continue -> inner_loop (i + 1) file_descr
      | `Stop -> `Stopped
    else `Interrupted_or_closed
  in
  (* [Fd.with_file_descr] is for [Raw_fd.set_nonblock_if_necessary].
     [with_file_descr_deferred] would be the more natural choice, but it doesn't call
     [set_nonblock_if_necessary]. *)
  match Fd.with_file_descr ~nonblocking:true fd (fun file_descr ->
    Fd.interruptible_every_ready_to fd read_or_write ~interrupt:(Ivar.read stop)
      (fun file_descr ->
         try match inner_loop 0 file_descr with
           | `Interrupted_or_closed -> ()
           | `Stopped -> Ivar.fill_if_empty stop ()
         with
         | Unix.Unix_error ((EAGAIN | EWOULDBLOCK | EINTR), _, _) -> ()
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
    (* Iobuf.sendto_nonblocking_no_sigpipe handles EAGAIN and EWOULDBLOCK by doing nothing
       rather than throwing an exception. *)
    ready_iter fd ~max_ready:default_retry ~stop `Write ~f:(fun file_descr ->
      let len_before = Iobuf.length buf in
      sendto buf file_descr addr;
      if len_before = Iobuf.length buf then `Continue else `Stop)
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
     address is chosen (i.e., an ephemeral port).  In almost all cases where we use this,
     we want a unique port, and hence prevent reuseaddr. *)
  let bind_addr = Socket.Address.Inet.create_bind_any ~port:0 in
  Socket.bind socket ~reuseaddr:false bind_addr
;;

let recvfrom_loop_with_buffer_replacement ?(config = Config.create ()) fd f =
  let stop = Ivar.create () in
  Config.stop config
  >>> Ivar.fill_if_empty stop;
  let buf = ref (Config.init config) in
  ready_iter ~stop ~max_ready:config.max_ready fd `Read ~f:(fun file_descr ->
    let addr = Iobuf.recvfrom_assume_fd_is_nonblocking !buf file_descr in
    match addr with
    | ADDR_UNIX dom ->
      fail !buf "Unix domain socket addresses not supported" dom <:sexp_of< string >>
    | ADDR_INET (host, port) ->
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
  ready_iter ~stop ~max_ready:config.max_ready fd `Read ~f:(fun file_descr ->
    Core.Syscall_result.Unit.ok_or_unix_error_exn
      (Iobuf.read_assume_fd_is_nonblocking !buf file_descr)
      ~syscall_name:"read";
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
      ?(on_wouldblock = (fun () -> ()))
      fd
      f
      ->
        let srcs : Core.Std.Unix.sockaddr array option =
          if create_srcs
          then Some
                 (Array.create ~len:(Array.length bufs)
                    (Socket.Address.to_sockaddr (`Inet (Unix.Inet_addr.bind_any, 0))))
          else None
        in
        let stop = Ivar.create () in
        Config.stop config
        >>> Ivar.fill_if_empty stop;
        ready_iter ~stop ~max_ready:config.max_ready fd `Read ~f:(fun file_descr ->
          let result = recvmmsg ?srcs file_descr bufs in
          if Unix.Syscall_result.Int.is_ok result then begin
            let count = Unix.Syscall_result.Int.ok_exn result in
            if count > Array.length bufs
            then
              failwithf
                "Unexpected result from Iobuf.recvmmsg_assume_fd_is_nonblocking: \
                 count (%d) > Array.length bufs (%d)"
                count
                (Array.length bufs)
                ()
            else begin
              for i = 0 to count - 1 do Config.before config bufs.(i) done;
              f ?srcs bufs ~count;
              for i = 0 to count - 1 do Config.after  config bufs.(i) done;
              `Continue
            end
          end else
            match Unix.Syscall_result.Int.error_exn result with
            | EWOULDBLOCK | EAGAIN ->
              on_wouldblock ();
              `Continue
            | error ->
              raise (Unix.Unix_error(error, "recvmmsg", "")))
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
      ?(on_wouldblock = (fun () -> ()))
      callback
      ->
        let stop = Ivar.create () in
        Config.stop config
        >>> Ivar.fill_if_empty stop;
        ready_iter ~stop ~max_ready:config.Config.max_ready fd `Read ~f:(fun file_descr ->
          let result = recvmmsg file_descr ~count:max_count bufs in
          if Unix.Syscall_result.Int.is_ok result then begin
            let count = Unix.Syscall_result.Int.ok_exn result in
            if count > Array.length bufs
            then
              failwithf
                "Unexpected result from \
                 Iobuf.recvmmsg_assume_fd_is_nonblocking_no_options: \
                 count (%d) > Array.length bufs (%d)"
                count
                (Array.length bufs)
                ()
            else begin
              for i = 0 to count - 1 do Config.before config bufs.(i) done;
              callback bufs ~count;
              for i = 0 to count - 1 do Config.after  config bufs.(i) done;
              `Continue
            end
          end else
            match Unix.Syscall_result.Int.error_exn result with
            | EWOULDBLOCK | EAGAIN ->
              on_wouldblock ();
              `Continue
            | error -> raise (Unix.Unix_error (error, "recvmmsg", "")))
        >>| function
        | (`Bad_fd | `Unsupported) as error ->
          failwiths "Udp.recvmmsg_no_sources_loop" (error, fd)
            <:sexp_of< [ `Bad_fd | `Unsupported ] * Fd.t >>
        | `Closed | `Interrupted -> ()))
;;
