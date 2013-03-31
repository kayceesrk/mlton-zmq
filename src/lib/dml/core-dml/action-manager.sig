(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Action helper
 *
 *)

signature ACTION_MANAGER =
sig

  type action_id = RepTypes.action_id
  type action = RepTypes.action
  type ptr = RepTypes.ptr


  val dummyAid    : action_id
  val newAid      : unit -> action_id
  (* val aidToPid    : action_id -> RepTypes.process_id *)
  val aidToPidInt : action_id -> int
  val aidToTidInt : action_id -> int
  val aidToTid    : action_id -> RepTypes.thread_id
  val aidToString : action_id -> string
  val aidToActNum : action_id -> int
  val isAidLocal  : action_id -> bool
  val getPrevAid  : action_id -> action_id
  val getNextAid  : action_id -> action_id
  val aidToPtr    : action_id -> ptr

  val actionToString : action -> string
end
