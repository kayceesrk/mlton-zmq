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
                                     aid: int}
  type node = unit DirectedGraph.Node.t

  val handleBegin : node -> unit
  val handleSpawn : {newTid : int} -> node
  val handleSend  : {cid : RepTypes.channel_id} -> unit
  val handleRecv  : {cid : RepTypes.channel_id} -> unit
  val setMatchAct : node * action_id -> unit
end
