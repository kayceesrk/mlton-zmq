(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

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
  val sockConnect    : socket * string -> unit
  val sockDisconnect : socket * string -> unit
  val sockBind       : socket * string -> unit
  val sockUnbind     : socket * string -> unit

  (* Management *)

  datatype socket_event = POLL_IN | POLL_OUT | POLL_IN_OUT | NO_EVENT

  val sockGetType                 : socket -> socket_kind
  val sockGetRcvMore              : socket -> bool
  val sockGetSndHwm               : socket -> int
  val sockGetRcvHwm               : socket -> int
  val sockGetAffinity             : socket -> Word64.word
  val sockGetIdentity             : socket -> Word8.word vector
  val sockGetRate                 : socket -> int
  val sockGetRecoveryIvl          : socket ->int
  val sockGetSndBuf               : socket -> int
  val sockGetRcvBuf               : socket -> int
  val sockGetLinger               : socket -> int
  val sockGetReconnectIvl         : socket -> int
  val sockGetReconnectIvlMax      : socket -> int
  val sockGetBacklog              : socket -> int
  val sockGetMaxMsgSize           : socket -> int
  val sockGetMulticastHops        : socket -> int
  val sockGetRcvTimeo             : socket -> int
  val sockGetSndTimeo             : socket -> int
  val sockGetIPV4Only             : socket -> bool
  val sockGetDelayAttachOnConnect : socket -> bool
  val sockGetFD                   : socket -> ('af,'sock_type) Socket.sock
  val sockGetEvents               : socket -> socket_event
  val sockGetLastEndpoint         : socket -> string
  val sockGetTCPKeepalive         : socket -> int
  val sockGetTCPKeepaliveIdle     : socket -> int
  val sockGetTCPKeepaliveCnt      : socket -> int
  val sockGetTCPKeepaliveIntvl    : socket -> int

  val sockSetSndHwm               : socket * int -> unit
  val sockSetRcvHwm               : socket * int -> unit
  val sockSetAffinity             : socket * Word64.word -> unit
  val sockSetSubscribe            : socket * Word8.word vector -> unit
  val sockSetUnsubscribe          : socket * Word8.word vector -> unit
  val sockSetRate                 : socket * int -> unit
  val sockSetRecoveryIvl          : socket * int -> unit
  val sockSetSndBuf               : socket * int -> unit
  val sockSetRcvBuf               : socket * int -> unit
  val sockSetLinger               : socket * int -> unit
  val sockSetReconnectIvl         : socket * int -> unit
  val sockSetReconnectIvlMax      : socket * int -> unit
  val sockSetBacklog              : socket * int -> unit
  val sockSetMaxMsgSize           : socket * int -> unit
  val sockSetMulticastHops        : socket * int -> unit
  val sockSetRcvTimeo             : socket * int -> unit
  val sockSetSndTimeo             : socket * int -> unit
  val sockSetIPV4Only             : socket * bool -> unit
  val sockSetDelayAttachOnConnect : socket * bool -> unit
  val sockSetRouterMandatory      : socket * int -> unit
  val sockSetXPubVerbose          : socket * int -> unit
  val sockSetTCPKeepalive         : socket * int -> unit
  val sockSetTCPKeepaliveIdle     : socket * int -> unit
  val sockSetTCPKeepaliveCnt      : socket * int -> unit
  val sockSetTCPKeepaliveIntvl    : socket * int -> unit
  val sockSetTCPAcceptFilter      : socket * Word8.word vector -> unit

  (* Communication *)
  datatype send_flag = S_DONT_WAIT | S_SEND_MORE | S_NONE

  val send : socket * 'a -> unit
  (* Prefix is used for subscriptions. Importantly, prefix is DROPPED by the
   * receiver, and only receives the message *)
  val sendWithPrefix : socket * 'a * Word8.word vector -> unit
  val sendWithPrefixAndFlag : socket * 'a * Word8.word vector * send_flag -> unit

  val recv    : socket -> 'a
  val recvNB  : socket -> 'a option

  (* Poll *)
  val poll : {ins : socket list, outs : socket list,
              inouts : socket list, timeout: int} ->
             {ins : socket list, outs: socket list,
              inouts : socket list}

  val proxy : {frontend: socket, backend: socket} -> unit
end
