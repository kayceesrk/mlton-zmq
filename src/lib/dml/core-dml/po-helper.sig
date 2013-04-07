(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Action helper
 *
 *)

signature PO_HELPER =
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

  val insertCommitNode : unit -> action_id
  val insertRollbackNode : unit -> action_id
  val handleInit  : {parentAid: action_id} -> action_id
  val handleSpawn : {childTid : RepTypes.thread_id} -> {spawnAid: action_id, spawnNode: node}
  val handleSend  : {cid : RepTypes.channel_id} -> comm_result
  val handleRecv  : {cid : RepTypes.channel_id} -> comm_result

  (* ---------------------- *)

  val setMatchAid : node -> action_id -> RepTypes.w8vec -> unit
  val getPrevNode : node -> node
  val isLastNode  : node -> bool
  val nodeToAction: node -> action

  val getFinalAction     : unit -> RepTypes.action
  val doOnUpdateLastNode : (unit -> unit) -> unit
  val isLastNodeMatched  : unit -> bool
  val isLastAidOnThread  : 'a CML.Scheduler.thread * action_id -> bool

  val saveCont    : (unit -> unit) -> unit
  (* Takes as input the last valid action number that is not part of a cycle.
   * This is used to create a cache for log-based recovery when the thread
   * resumes *)
  val restoreCont : int -> unit
  val inNonSpecExecMode : unit -> bool
end
