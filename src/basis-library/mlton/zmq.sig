signature MLTON_ZMQ =
sig

  (* Context Management *)
  type context
  datatype context_option = IO_THREADS | MAX_SOCKETS

  val ctxNew     : unit -> context
  val ctxDestroy : context -> unit
  val ctxSetOpt  : context * context_option * int -> unit
  val ctxGetOpt  : context * context_option -> int

  (* Socket Management *)
  type socket
  datatype socket_kind = Pair | Pub | Sub | Req | Rep
                       | Dealer | Router | Pull | Push
                       | XPub | XSub

  (* Creation and Destruction *)
  val sockCreate : context * socket_kind -> socket
  val sockClose  : socket -> unit

  (* Wiring *)
  val sockConnect : socket * string -> unit
  val sockBind    : socket * string -> unit

  (* Management *)

end
