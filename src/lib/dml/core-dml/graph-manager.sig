(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature GRAPH_MANAGER =
sig
  type action_id = RepTypes.action_id
  type action = RepTypes.action
  type node

  datatype comm_result = UNCACHED of {waitNode: node, actAid: action_id}
                       | CACHED of RepTypes.w8vec

  (* ---------------------
   * Inserting new actions
   * ---------------------
   *)

  val insertCommitNode    : unit -> action_id
  val insertRollbackNode  : unit -> action_id
  val insertNoopNode      : unit -> action_id
  val handleInit  : {parentAid: action_id option} -> action_id
  val handleSpawn : {childTid : RepTypes.thread_id} -> {spawnAid: action_id, spawnNode: node}
  val handleSend  : {cid : RepTypes.channel_id} -> comm_result
  val handleRecv  : {cid : RepTypes.channel_id} -> comm_result

  (* ---------------------- *)

  val setMatchAid : {waitNode: node, actAid: action_id,
                     matchAid: action_id, value: RepTypes.w8vec} -> unit
  val getPrevNode : node -> node
  val isLastNode  : node -> bool
  val nodeToAction: node -> action
  val getValue    : node -> RepTypes.w8vec option

  val getFinalAction     : unit -> RepTypes.action
  val doOnUpdateLastNode : (unit -> unit) -> unit
  val isLastNodeMatched  : unit -> bool
  val getWaitAid : {actAid: action_id, waitNode: node} -> action_id

  val saveCont    : (unit -> unit) -> unit
  (* Takes as input the last valid action number that is not part of a cycle.
   * This is used to create a cache for log-based recovery when the thread
   * resumes *)
  val restoreCont : int -> unit
  val inNonSpecExecMode : unit -> bool

  val abortChoice : unit -> unit
  val commitChoice : action_id -> unit
end
