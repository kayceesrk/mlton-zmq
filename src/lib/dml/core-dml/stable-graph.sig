(* stable-graph.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)

signature STABLE_GRAPH =
sig
  datatype action_id = ACTION_ID of {pid: RepTypes.process_id,
                                     tid: RepTypes.thread_id,
                                     rid: int,
                                     aid: int}
  type node = unit DirectedGraph.Node.t

  val dummyAid    : action_id
  val aidToPidInt : action_id -> int
  val aidToTidInt : action_id -> int
  val aidToString : action_id -> string

  val handleInit  : {parentAid: action_id} -> unit
  val handleSpawn : {childTid : RepTypes.thread_id} -> action_id
  val handleSend  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}
  val handleRecv  : {cid : RepTypes.channel_id} -> {waitNode: node, actAid: action_id}
  val setMatchAct : node -> action_id -> unit


  val rhNodeToThreads : {startNode  : node,
                         tid2tid    : RepTypes.thread_id -> CML.thread_id option} ->
                        {localRestore     : CML.thread_id list,
                         localKill        : CML.thread_id list,
                         remoteRollbacks  : action_id list}
end
