structure MLtonZMQ : MLTON_ZMQ =
struct

  structure SysCall = PosixError.SysCall

  (* KC: For readability, primitive function names must be made to correspond
   * to the C function names they correspond to *)
  structure Prim = PrimitiveFFI.MLton.ZMQ

  (* Context Management *)
  type context = C_Pointer.t
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
    SysCall.simpleResultRestart' ({errVal = CUtil.C_Pointer.null}, Prim.ctx_new)

  (* Using simpleRestart due to the following reasons
  * (1) errVal is default (~1).
  * (2) No result is expected.
  *)
  fun ctxDestroy context =
    SysCall.simpleRestart (fn () => Prim.ctx_destroy context)

  fun ctxSetOpt (ctx, opt, v) =
    SysCall.simpleRestart (fn () => Prim.ctx_set (ctx, contextOptionToInt opt, v))

  fun ctxGetOpt (ctx, opt) =
    SysCall.simpleResultRestart (fn () => Prim.ctx_get (ctx, contextOptionToInt opt))


  (* Sockets *)

  datatype socket_kind = Pair | Pub | Sub | Req | Rep
                       | Dealer | Router | Pull | Push
                       | XPub | XSub

  datatype socket = SOCKET of {hndl : C_Pointer.t, kind : socket_kind}

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
    val hndl = SysCall.simpleResultRestart' ({errVal = CUtil.C_Pointer.null},
                                              fn () => Prim.socket (ctxt, socketKindToInt kind))
  in
    SOCKET {hndl = hndl, kind = kind}
  end

  fun sockClose (SOCKET {hndl, ...}) =
    SysCall.simpleRestart (fn () => Prim.close hndl)

  fun sockConnect (SOCKET {hndl, ...}, endPoint) =
    SysCall.simpleRestart (fn () => Prim.connect (hndl, NullString.nullTerm endPoint))

  fun sockBind (SOCKET {hndl,...}, endPoint) =
    SysCall.simpleRestart (fn () => Prim.bind (hndl, NullString.nullTerm endPoint))
end
