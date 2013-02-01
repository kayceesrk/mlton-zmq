(* communication-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Communication Manager.
 *
 *)

signature COMM_MANAGER =
sig
  type msg = RepTypes.msg

  val msgToString : msg -> string
  val msgSend     : msg -> unit
  val msgSendSafe : msg -> unit
  val msgRecv     : unit -> msg option
  val msgRecvSafe : unit -> msg option
end
