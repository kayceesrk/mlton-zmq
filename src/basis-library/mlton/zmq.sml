(* Copyright (C) 2010 Matthew Fluet.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure MLtonZMQ : MLTON_ZMQ =
struct

  structure SysCall = PosixError.SysCall

  exception ZMQError of string * Posix.Error.syserror option

  fun exnWrapper v = v handle PosixError.SysErr (str, errOpt) => raise ZMQError (str, errOpt)

  (* KC: For readability, primitive function names must be made to correspond
   * to the C function names they correspond to *)
  structure Prim =
  struct
    open PrimitiveFFI.MLton.ZMQ

    val send = _prim "MLton_ZMQ_Send" : 'a ref * Word8.word vector * C_ZMQ_Socket.t * C_Int.t -> (C_Int.t) C_Errno.t;
    val deserializeZMQMsg = _prim "MLton_deserializeZMQMsg" : C_ZMQ_Message.t -> 'a;
  end

  (* Context Management *)
  type context = C_ZMQ_Context.t
  datatype context_option = IO_THREADS | MAX_SOCKETS

  fun contextOptionToInt opt =
    case opt of
          IO_THREADS => Prim.IO_THREADS
        | MAX_SOCKETS => Prim.MAX_SOCKETS

  (* Need to use simpleResultRestart' for the following reasons
  * (1) We need a return value.
  * (2) We need to specify an explicit errVal (null in this case), which is
  *     not the default errVal of ~1.
  * (3) We need to restart the call if we are interrupted by a signal. We
  *     expect signals *always* due to MultiMLton's user-level threading.
  *)
  fun ctxNew () =
    exnWrapper (SysCall.simpleResultRestart' ({errVal = CUtil.C_Pointer.null}, Prim.ctx_new))

  (* Using simpleRestart due to the following reasons
  * (1) errVal is default (~1).
  * (2) No result is expected.
  *)
  fun ctxDestroy context =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.ctx_destroy context))

  fun ctxSetOpt (ctx, opt, v) =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.ctx_set (ctx, contextOptionToInt opt, v)))

  fun ctxGetOpt (ctx, opt) =
    exnWrapper (SysCall.simpleResultRestart (fn () => Prim.ctx_get (ctx, contextOptionToInt opt)))


  (* Sockets *)

  datatype socket_kind = Pair | Pub | Sub | Req | Rep
                       | Dealer | Router | Pull | Push
                       | XPub | XSub

  datatype socket = SOCKET of {hndl : C_ZMQ_Socket.t, kind : socket_kind}

  fun socketKindToInt k =
    case k of
         Pair => Prim.PAIR
       | Pub => Prim.PUB
       | Sub => Prim.SUB
       | Req => Prim.REQ
       | Rep => Prim.REP
       | Dealer => Prim.DEALER
       | Router => Prim.ROUTER
       | Pull => Prim.PULL
       | Push => Prim.PUSH
       | XPub => Prim.XPUB
       | XSub => Prim.XSUB

  fun sockCreate (ctxt, kind) =
  let
    val hndl = exnWrapper (SysCall.simpleResultRestart'
                            ({errVal = CUtil.C_Pointer.null},
                              fn () => Prim.socket (ctxt, socketKindToInt kind)))
  in
    SOCKET {hndl = hndl, kind = kind}
  end

  fun sockClose (SOCKET {hndl, ...}) =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.close hndl))

  fun sockConnect (SOCKET {hndl, ...}, endPoint) =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.connect (hndl, NullString.nullTerm endPoint)))

  fun sockDisconnect (SOCKET {hndl, ...}, endPoint) =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.disconnect (hndl, NullString.nullTerm endPoint)))

  fun sockBind (SOCKET {hndl,...}, endPoint) =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.bind (hndl, NullString.nullTerm endPoint)))

  fun sockUnbind (SOCKET {hndl,...}, endPoint) =
    exnWrapper (SysCall.simpleRestart (fn () => Prim.unbind (hndl, NullString.nullTerm endPoint)))

  (* Socket Options *)

  local
    (* host byte order *)
    type optvalVec = Word8.word vector
    type optvalArr = Word8.word array

    val bitsPerByte = 8

    val isBigEndian = Primitive.MLton.Platform.Arch.hostIsBigEndian



    (* int *)
    val intLen = Int.quot (C_Int.precision', bitsPerByte)
    fun unmarshalInt (wa: optvalArr) : C_Int.int =
        let
          fun loop (i, acc) =
              if i >= intLen
                then acc
                else let
                        val w =
                            Array.sub
                            (wa, if isBigEndian
                                    then i
                                    else (intLen - 1) - i)
                        val w = C_Int.castFromSysWord (Word8.castToSysWord w)
                      in
                        loop (i + 1, C_Int.orb (w, C_Int.<< (acc, 0w4)))
                      end
        in
          loop (0, 0)
        end
    fun marshalInt (i: C_Int.int) : optvalVec =
        let
          val wa = Array.array (intLen, 0wx0)
          fun loop (i, acc) =
              if i >= intLen
                then ()
                else let
                        val w = Word8.castFromSysWord (C_Int.castToSysWord acc)
                        val () =
                            Array.update
                            (wa, if isBigEndian
                                    then (intLen - 1) - i
                                    else i, w)
                      in
                        loop (i + 1, C_Int.>> (acc, 0w4))
                      end
        in
          loop (0, i)
          ; Array.vector wa
        end

    (* bool *)
    val boolLen = intLen
    fun unmarshalBool (wa: optvalArr) : bool =
        if (unmarshalInt wa) = 0 then false else true
    fun marshalBool (b: bool) : optvalVec =
        marshalInt (if b then 1 else 0)

    (* word64 *)
    val word64Len = 8 (* Word64.sizeInBits / bitsPerByte *)
    fun unmarshalWord64 (wa: optvalArr) : Word64.word =
        let
          fun loop (i, acc) =
              if i >= word64Len
                then acc
                else let
                        val w =
                            Array.sub
                            (wa, if isBigEndian
                                    then i
                                    else (word64Len - 1) - i)
                        val w = Word64.castFromSysWord (Word8.castToSysWord w)
                      in
                        loop (i + 1, Word64.orb (w, Word64.<< (acc, 0w4)))
                      end
        in
          loop (0, 0wx0)
        end
    fun marshalWord64 (i: Word64.word) : optvalVec =
        let
          val wa = Array.array (word64Len, 0wx0)
          fun loop (i, acc) =
              if i >= word64Len
                then ()
                else let
                        val w = Word8.castFromSysWord (Word64.castToSysWord acc)
                        val () =
                            Array.update
                            (wa, if isBigEndian
                                    then (word64Len - 1) - i
                                    else i, w)
                      in
                        loop (i + 1, Word64.>> (acc, 0w4))
                      end
        in
          loop (0, i)
          ; Array.vector wa
        end


    (* Time *)
    val optTimeLen: int = intLen
    fun unmarshalOptTime (wa: optvalArr) : Time.time option =
    let
      val milliSecs = unmarshalInt wa
    in
      SOME (Time.fromMilliseconds (C_Int.toLarge milliSecs))
    end
    fun marshalOptTime (to: Time.time option) : optvalVec =
    let
      val millisecs = case to of
                           NONE => ~1
                         | SOME t => C_Int.fromLarge (Time.toMilliseconds t)
    in
      marshalInt millisecs
    end


    fun make (optlen: int,
              marshal: 'a -> optvalVec,
              unmarshal: optvalArr -> 'a) =
      let
        fun getSockOpt optname (SOCKET{hndl, ...}) : 'a =
        let
          val optval = Array.array (optlen, 0wx0)
          val optlen' = ref (C_Size.fromInt optlen)
          val () =
            exnWrapper (SysCall.simpleRestart
            (fn () => Prim.getSockOpt (hndl, optname, optval, optlen')))
          val () =
            if C_Size.toInt (!optlen') <> optlen
            then raise (Fail "ZMQ.getSockOpt: optlen' <> optlen")
            else ()
        in
          unmarshal optval
        end

        fun setSockOpt optname (SOCKET {hndl, ...}, optval: 'a) : unit =
        let
          val optval = marshal optval
          val optlen' = C_Size.fromInt optlen
          val () =
            exnWrapper (SysCall.simpleRestart
            (fn () => Prim.setSockOpt (hndl, optname, optval, optlen')))
        in
          ()
        end
      in
        (getSockOpt, setSockOpt)
      end
  in
    val (getSockOptInt, setSockOptInt) = make (intLen, marshalInt, unmarshalInt)
    val (getSockOptBool, setSockOptBool) = make (intLen, marshalBool, unmarshalBool)
    val (getSockOptWord64, setSockOptWord64) = make (word64Len, marshalWord64, unmarshalWord64)
    val (getSockOptTime, setSockOptTime) = make (intLen, marshalOptTime, unmarshalOptTime)
  end

  datatype socket_event = POLL_IN | POLL_OUT | POLL_IN_OUT | POLL_ERR

  fun sockEventFromInt event =
    if event = Prim.POLLIN then POLL_IN
    else if event = Prim.POLLOUT then POLL_OUT
    else if event = C_Int.orb (Prim.POLLIN, Prim.POLLOUT) then POLL_IN_OUT
    else POLL_ERR


  fun getSockOptWord8Vector optname optlen (SOCKET {hndl, ...}) : Word8.word vector =
  let
    val optval = Array.array (optlen, 0wx0)
    val optlen' = ref (C_Size.fromInt optlen)
    val () =
      exnWrapper (SysCall.simpleRestart
      (fn () => Prim.getSockOpt (hndl, optname, optval, optlen')))
    val optlen = C_Size.toInt (!optlen')
  in
    ArraySlice.vector (ArraySlice.slice (optval, 0, SOME optlen))
  end

  fun setSockOptWord8Vector optname (SOCKET {hndl, ...}, optval : Word8.word vector) : unit =
  let
    val optlen = Vector.length optval
  in
    exnWrapper (SysCall.simpleRestart
    (fn () => Prim.setSockOpt (hndl, optname, optval, C_Size.fromInt optlen)))
  end

  val sockGetType = fn (SOCKET {kind, ...}) => kind
  val sockGetRcvMore = getSockOptBool Prim.RCVMORE
  val sockGetSndHwm = getSockOptInt Prim.SNDHWM
  val sockGetRcvHwm = getSockOptInt Prim.RCVHWM
  val sockGetAffinity = getSockOptWord64 Prim.AFFINITY
  val sockGetIdentity = getSockOptWord8Vector Prim.IDENTITY 256
  val sockGetRate = getSockOptInt Prim.RATE
  val sockGetRecoveryIvl = getSockOptInt Prim.RECOVERY_IVL
  val sockGetSndBuf = getSockOptInt Prim.SNDBUF
  val sockGetRcvBuf = getSockOptInt Prim.RCVBUF
  val sockGetLinger = getSockOptInt Prim.LINGER
  val sockGetReconnectIvl = getSockOptInt Prim.RECONNECT_IVL
  val sockGetReconnectIvlMax = getSockOptInt Prim.RECONNECT_IVL_MAX
  val sockGetBacklog = getSockOptInt Prim.BACKLOG
  (* TODO: Implement getSockOptInt64 *)
  val sockGetMaxMsgSize = getSockOptInt Prim.MAXMSGSIZE
  val sockGetMulticastHops = getSockOptInt Prim.MULTICAST_HOPS
  val sockGetRcvTimeo = getSockOptInt Prim.RCVTIMEO
  val sockGetSndTimeo = getSockOptInt Prim.SNDTIMEO
  val sockGetIPV4Only = getSockOptBool Prim.IPV4ONLY
  val sockGetDelayAttachOnConnect = getSockOptBool Prim.DELAY_ATTACH_ON_CONNECT
  val sockGetFD = fn s => Socket.fromRep (C_Sock.castFromSysWord (C_Int.castToSysWord (getSockOptInt Prim.FD s)))
  val sockGetEvents = fn s => sockEventFromInt (getSockOptInt Prim.EVENTS s)
  val sockGetLastEndpoint = fn s => Byte.bytesToString (Word8Vector.fromPoly (getSockOptWord8Vector Prim.LAST_ENDPOINT 256 s))
  val sockGetTCPKeepalive = getSockOptInt Prim.TCP_KEEPALIVE
  val sockGetTCPKeepaliveIdle = getSockOptInt Prim.TCP_KEEPALIVE_IDLE
  val sockGetTCPKeepaliveCnt = getSockOptInt Prim.TCP_KEEPALIVE_CNT
  val sockGetTCPKeepaliveIntvl = getSockOptInt Prim.TCP_KEEPALIVE_INTVL

  val sockSetSndHwm = setSockOptInt Prim.SNDHWM
  val sockSetRcvHwm = setSockOptInt Prim.RCVHWM
  val sockSetAffinity = setSockOptWord64 Prim.AFFINITY
  val sockSetSubscribe = setSockOptWord8Vector Prim.SUBSCRIBE
  val sockSetUnsubscribe = setSockOptWord8Vector Prim.UNSUBSCRIBE
  val sockSetRate = setSockOptInt Prim.RATE
  val sockSetRecoveryIvl = setSockOptInt Prim.RECOVERY_IVL
  val sockSetSndBuf = setSockOptInt Prim.SNDBUF
  val sockSetRcvBuf = setSockOptInt Prim.RCVBUF
  val sockSetLinger = setSockOptInt Prim.LINGER
  val sockSetReconnectIvl = setSockOptInt Prim.RECONNECT_IVL
  val sockSetReconnectIvlMax = setSockOptInt Prim.RECONNECT_IVL_MAX
  val sockSetBacklog = setSockOptInt Prim.BACKLOG
  (* TODO: Implement setSockOptInt64 *)
  val sockSetMaxMsgSize = setSockOptInt Prim.MAXMSGSIZE
  val sockSetMulticastHops = setSockOptInt Prim.MULTICAST_HOPS
  val sockSetRcvTimeo = setSockOptInt Prim.RCVTIMEO
  val sockSetSndTimeo = setSockOptInt Prim.SNDTIMEO
  val sockSetIPV4Only = setSockOptBool Prim.IPV4ONLY
  val sockSetDelayAttachOnConnect = setSockOptBool Prim.DELAY_ATTACH_ON_CONNECT
  val sockSetRouterMandatory = setSockOptInt Prim.ROUTER_MANDATORY
  val sockSetXPubVerbose = setSockOptInt Prim.XPUB_VERBOSE
  val sockSetTCPKeepalive = setSockOptInt Prim.TCP_KEEPALIVE
  val sockSetTCPKeepaliveIdle = setSockOptInt Prim.TCP_KEEPALIVE_IDLE
  val sockSetTCPKeepaliveCnt = setSockOptInt Prim.TCP_KEEPALIVE_CNT
  val sockSetTCPKeepaliveIntvl = setSockOptInt Prim.TCP_KEEPALIVE_INTVL
  val sockSetTCPAcceptFilter = setSockOptWord8Vector Prim.TCP_ACCEPT_FILTER

  (* Sends and Receives *)

  datatype send_flag = S_DONT_WAIT | S_SEND_MORE | S_NONE

  fun sendFlgToInt flg =
    case flg of
         S_DONT_WAIT => Prim.DONTWAIT
       | S_SEND_MORE => Prim.SNDMORE
       | S_NONE => 0

  datatype recv_flag = R_DONT_WAIT | R_NONE

  fun recvFlgToInt flg =
    case flg of
         R_DONT_WAIT => Prim.DONTWAIT
       | R_NONE => 0

  (* Send always works with references *)
  fun sendWithPrefixAndFlag (SOCKET {hndl, ...}, msg, prefix, flg) =
      exnWrapper (SysCall.simpleRestart
      (fn () => Prim.send (ref msg, prefix, hndl, sendFlgToInt flg)))

  (* Since all sent messages are references, we need to dereference the
    * deserialized message to get the actual value *)
  fun recvWithFlag (SOCKET {hndl, ...}, flg) =
    let
      val zmqMsg =
        exnWrapper (SysCall.simpleResultRestart'
        ({errVal = CUtil.C_Pointer.null}, fn () => Prim.recv (hndl, recvFlgToInt flg)))
    in
      !(Prim.deserializeZMQMsg zmqMsg)
    end

  fun sendWithPrefix (sock, msg, prefix) = sendWithPrefixAndFlag (sock, msg, prefix, S_NONE)
  fun send (sock, msg) = sendWithPrefix (sock, msg, Vector.tabulate (0, fn _ => 0wx0 : Word8.word))
  fun recv (sock) = recvWithFlag (sock, R_NONE)
end
