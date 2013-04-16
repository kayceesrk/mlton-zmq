(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature ORCHESTRATOR =
sig

  datatype caller_kind = Daemon | Client

  val clientDaemon : unit -> unit
  val saveCont : unit -> unit
  val restoreCont : int -> unit
  val processSend : {channel: RepTypes.channel_id,
                     sendActAid: RepTypes.action_id,
                     sendWaitNode: GraphManager.node,
                     value: RepTypes.w8vec,
                     callerKind: caller_kind} -> unit
  val processRecv : {channel: RepTypes.channel_id,
                     recvActAid: RepTypes.action_id,
                     recvWaitNode: GraphManager.node,
                     callerKind: caller_kind} -> RepTypes.w8vec
  val inNonSpecExecMode : unit -> bool
  val exitDaemon : bool ref
  val processRollbackMsg: int RepTypes.PTRDict.dict -> RepTypes.action -> unit
  val connect : {sink: string, source: string,
                 processId: int, numPeers: int} -> unit
  val startProxy : {frontend: string, backend: string} -> unit
end
