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

let fail buf message a sexp_of_a =
  (* Render buffers immediately, before we have a chance to change them. *)
  failwiths message (a, <:sexp_of< (_, _) Iobuf.t >> buf)
    (Tuple.T2.sexp_of_t sexp_of_a ident)
;;

let failf buf =
  ksprintf (fun message ->
    failwiths message (<:sexp_of< (_, _) Iobuf.t >> buf)
      <:sexp_of< Sexp.t >>)
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

let sendto () =
  Iobuf.sendto_nonblocking_no_sigpipe ()
  |> Or_error.map ~f:(fun sendto ->
    (fun fd buf addr ->
       let addr = Unix.Socket.Address.to_sockaddr addr in
       let stop = Ivar.create () in
       (* Iobuf.sendto_nonblocking_no_sigpipe returns EAGAIN and EWOULDBLOCK through an
          option rather than an exception. *)
       Fd.ready_fold fd ~stop:(Ivar.read stop) ~init:`None `Write ~f:(fun _ desc ->
         match sendto buf desc addr with
         | None -> `None
         | Some _ -> Ivar.fill stop (); `Some)
       >>| function
       | `None -> fail buf "Closed" addr <:sexp_of< Core.Std.Unix.sockaddr >>
       | `Some -> ()))
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

let recvfrom_loop_with_buffer_replacement ?(config = Config.create ()) fd callback =
  let stop = Config.stop config in
  Fd.ready_fold ~stop fd ~init:(Config.init config) `Read ~f:(fun buf desc ->
    let buf_len_before = Iobuf.length buf in
    let len, addr = Iobuf.recvfrom_assume_fd_is_nonblocking buf desc in
    if len <> buf_len_before - Iobuf.length buf
    then
      failf buf
        "Unexpected result from Iobuf.recvfrom_assume_fd_is_nonblocking: \
         len (%d) <> buf_len_before (%d) - Iobuf.length buf"
        len
        buf_len_before;
    match addr with
    | Core.Std.Unix.ADDR_UNIX dom ->
      fail buf "Unix domain socket addresses not supported" dom <:sexp_of< string >>
    | Core.Std.Unix.ADDR_INET (host, port) ->
      Config.before config buf;
      let buf = callback buf (`Inet (host, port)) in
      Config.after  config buf;
      buf)
;;
let recvfrom_loop ?config fd callback =
  recvfrom_loop_with_buffer_replacement ?config fd (fun b a -> callback b a; b)
  >>| (ignore : (_, _) Iobuf.t -> unit)
;;

(* We don't care about the address, so read instead of recvfrom. *)
let read_loop_with_buffer_replacement ?(config = Config.create ()) fd callback =
  let stop = Config.stop config in
  Fd.ready_fold ~stop fd ~init:(Config.init config) `Read ~f:(fun buf desc ->
    let buf_len_before = Iobuf.length buf in
    let len = Iobuf.read_assume_fd_is_nonblocking buf desc in
    if len <> buf_len_before - Iobuf.length buf
    then
      failf buf
        "Unexpected result from Iobuf.read_assume_fd_is_nonblocking: \
         len (%d) <> buf_len_before (%d) - Iobuf.length buf"
        len
        buf_len_before;
    Config.before config buf;
    let buf = callback buf in
    Config.after  config buf;
    buf)
;;
let read_loop ?config fd callback =
  read_loop_with_buffer_replacement ?config fd (fun b -> callback b; b)
  >>| (ignore : (_, _) Iobuf.t -> unit)
;;

let recvmmsg_loop =
  Or_error.map Iobuf.recvmmsg_assume_fd_is_nonblocking ~f:(fun recvmmsg ->
    (fun
      ?(config = Config.create ())
      fd
      ?(create_srcs = false)
      (* There's a 64kB threshold for the total buffer size before releasing the lock in
         ../../../core/lib/recvmmsg.c.  This is a good number to stay below. *)
      ?(max_count = 64 * 1024 / Config.capacity config)
      ?(bufs =
        Array.init max_count ~f:(function
          | 0 -> Config.init config
          | _ -> Iobuf.create ~len:(Iobuf.length (Config.init config))))
      callback
      ->
        let srcs : Core.Std.Unix.sockaddr array option =
          if create_srcs
          then
            Some
              (Array.create ~len:(Array.length bufs)
                 (Socket.Address.to_sockaddr (`Inet (Unix.Inet_addr.bind_any, 0))))
          else None
        in
          let stop = Config.stop config in

        Fd.ready_fold ~stop fd ~init:() `Read ~f:(fun () desc ->
          let count = recvmmsg ?srcs desc bufs in
          if count > Array.length bufs
          then
            failwithf
              "Unexpected result from Iobuf.recvmmsg_assume_fd_is_nonblocking: \
               count (%d) > Array.length bufs (%d)"
              count
              (Array.length bufs)
              ();
          for i = 0 to count - 1 do Config.before config bufs.(i) done;
          callback ?srcs bufs ~count;
          for i = 0 to count - 1 do Config.after  config bufs.(i) done)))
;;

let bind_to_interface_exn =
  Linux_ext.bind_to_interface
  |> Or_error.map ~f:(fun bind_to_interface ->
    (fun ~ifname fd ->
       Fd.with_file_descr_exn fd (fun fd ->
         bind_to_interface fd (`Interface_name ifname))))
;;
