(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)

signature ACTION_MANAGER =
sig

  type action_id = RepTypes.action_id

  type node

  val dummyAid    : unit -> action_id
  val aidToPidInt : action_id -> int
  val aidToTidInt : action_id -> int
  val aidToTid    : action_id -> RepTypes.thread_id
  val aidToString : action_id -> string

  val insertCommitRollbackNode : unit -> action_id
  val handleInit  : {parentAid: action_id} -> action_id
  val handleSpawn : {childTid : RepTypes.thread_id} -> action_id
  val handleSend  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}
  val handleRecv  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}

  val setMatchAid : node -> action_id -> unit
  val getMatchAid : node -> action_id
  val removeMatchAid : node -> unit

  val isAidLocal : action_id -> bool
  val getPrevAid : action_id -> action_id
  val getNextAid : action_id -> action_id
  val getLastAid : unit -> action_id

  structure ActionIdOrdered : ORDERED where type t = action_id
  structure AISS : SET  where type elem = action_id
  structure AISD : DICT where type key = action_id

  val saveCont    : (unit -> unit) -> unit
  val restoreCont : unit -> unit
end
