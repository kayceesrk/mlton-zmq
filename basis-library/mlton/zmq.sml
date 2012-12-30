structure MLtonZMQ : MLTON_ZMQ =
struct

  structure SysCall = PosixError.SysCall

  structure Prim = PrimitiveFFI.MLton.ZMQ

  (* Context Management *)

  type context = C_Pointer.t

  (* Need to use simpleResultRestart' for the following reasons
   * (1) We need a return value.
   * (2) We need to specify an explicit errVal (null in this case), which is
   *     not the default errVal of ~1.
   * (3) We need to restart the call if we are interrupted by a signal. We
   *     expect signals *always* due to MultiMLton's user-level threading.
   *)
  val init = fn () => SysCall.simpleResultRestart'
                                ({errVal = CUtil.C_Pointer.null}, Prim.init)

  (* Using simpleRestart due to the following reasons
   * (1) errVal is default (~1).
   * (2) No result is expected.
   *)
  val term = fn context => SysCall.simpleRestart
                             (fn () => Prim.term context)
end
