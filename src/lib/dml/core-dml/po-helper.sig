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

  val insertCommitRollbackNode : unit -> action_id
  val handleInit  : {parentAid: action_id} -> action_id
  val handleSpawn : {childTid : RepTypes.thread_id} -> {spawnAid: action_id, spawnNode: node}
  val handleSend  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}
  val handleRecv  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}

  val setMatchAid : node -> action_id -> unit
  val getMatchAid : node -> action_id
  val removeMatchAid : node -> unit
  val getPrevNode : node -> node
  val getLastAid  : unit -> action_id

  val handleFinalizingSatedNode : node -> unit
  val saveCont    : (unit -> unit) -> unit
  val restoreCont : unit -> unit
end
