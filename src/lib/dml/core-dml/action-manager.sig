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

  val newAid      : unit -> action_id
  val dummyAid    : unit -> action_id
  val aidToPidInt : action_id -> int
  val aidToTidInt : action_id -> int
  val aidToTid    : action_id -> RepTypes.thread_id
  val aidToString : action_id -> string
  val isAidLocal  : action_id -> bool
  val getPrevAid  : action_id -> action_id
  val getNextAid  : action_id -> action_id

  val actionToString : action -> string

  structure ActionIdOrdered : ORDERED where type t = action_id
  structure AISS : SET  where type elem = action_id
  structure AISD : DICT where type key = action_id
end
