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

  val insertCommitNode : unit -> action_id
  val insertRollbackNode : unit -> action_id
  val handleInit  : {parentAid: action_id} -> action_id
  val handleSpawn : {childTid : RepTypes.thread_id} -> {spawnAid: action_id, spawnNode: node}
  val handleSend  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}
  val handleRecv  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}

  val setMatchAid : node -> action_id -> unit
  val getPrevNode : node -> node

  val sendToArbitrator : node -> unit
  val getFinalAction   : unit -> RepTypes.action
  val doOnUpdateLastNode : (unit -> unit) -> unit
  val isLastNodeMatched  : unit -> bool

  val inNonSpecExecMode : unit -> bool
  val saveCont    : (unit -> unit) -> unit
  val restoreCont : unit -> unit

  (* Given any node, getEarliestAction returns the earliest action in the
   * thread to which the given node belongs to. The earliest action is a
   * begin, commit or rollback action. *)
  val getEarliestAction : node -> action

  val isLastAidOnThread : 'a CML.Scheduler.thread * action_id -> bool
end
