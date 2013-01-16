(* stable-graph.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)


structure StableGraph : STABLE_GRAPH =
struct
  structure G = DirectedGraph
  structure N = G.Node
  structure E = G.Edge
  structure S = CML.Scheduler
  open RepTypes

  (********************************************************************
   * Graph
   *******************************************************************)

  val depGraph = G.new ()
  type node = unit DirectedGraph.Node.t

  (********************************************************************
   * Action
   *******************************************************************)

  (* provides a unique action id across the program *)
  datatype action_id = ACTION_ID of {pid: process_id, tid: thread_id, rid: int, aid: int}

  fun aidToString (ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid, aid}) =
    concat [Int.toString pid, ":", Int.toString tid, ":", Int.toString rid, ":", Int.toString aid]

  fun newAid () =
  let
    val pid = !processId
    val tid = S.tidInt ()
    val aid = S.tidNextActionNum ()
    val rid = S.tidRev ()
  in
    ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid = rid, aid = aid}
  end


  datatype action_type = SEND_ACT of {cid: channel_id, matchAct: action_id option}
                       | RECV_ACT of {cid: channel_id, matchAct: action_id option}
                       | SPAWN_ACT of {childAct: action_id}
                       | BEGIN_ACT

  fun aidToPidInt (ACTION_ID {pid = ProcessId pidInt, ...}) = pidInt
  fun aidToTidInt (ACTION_ID {tid = ThreadId tidInt, ...}) = tidInt

  fun actTypeToString at =
    case at of
         SEND_ACT {cid = ChannelId cstr, matchAct = NONE} => concat ["S (",cstr,",*)"]
       | SEND_ACT {cid = ChannelId cstr, matchAct = SOME act} => concat ["S (",cstr,",",aidToString act,")"]
       | RECV_ACT {cid = ChannelId cstr, matchAct = NONE} => concat ["R (",cstr,",*)"]
       | RECV_ACT {cid = ChannelId cstr, matchAct = SOME act} => concat ["R (",cstr,",",aidToString act,")"]
       | SPAWN_ACT {childAct} => concat ["F (", aidToString childAct, ")"]
       | BEGIN_ACT => "B"

  datatype action = ACTION of {aid: action_id, act: action_type}

  fun actionToString (ACTION {aid, act}) = concat ["[",aidToString aid,",",actTypeToString act,"]"]



  (********************************************************************
   * Node management
   *******************************************************************)

  val {get = getNodeEnv: unit N.t -> action,
       set = setNodeEnv, ...} =
        Property.getSet (N.plist, Property.initRaise ("StableGraph.nodeEnv", N.layout))

  fun nodeToString (node) =
  let
    val act = getNodeEnv node
  in
    actionToString act
  end

  fun getCurrentNode () =
    case !(S.tidNode ()) of
         NONE => raise Fail "StableGraph.getCurrentAct"
       | SOME n => n

  fun setCurrentNode node =
  let
    val nodeRef = S.tidNode ()
  in
    nodeRef := node
  end

  fun addProgramOrderEdge newNode =
  let
    val currentNode = getCurrentNode ()
    val _ = G.addEdge (depGraph, {from = currentNode, to = newNode})
  in
    setCurrentNode (SOME newNode)
  end

  fun assertIsBeginNode node =
    case (getNodeEnv node) of
         ACTION {act = BEGIN_ACT, ...} => ()
       | _ => raise Fail "StableGraph.assertIsBeginNode"

  fun assertIsSendNode node =
    case (getNodeEnv node) of
         ACTION {act = SEND_ACT _, ...} => ()
       | _ => raise Fail "StableGraph.assertIsSendNode"

  fun assertIsRecvNode node =
    case (getNodeEnv node) of
         ACTION {act = RECV_ACT _, ...} => ()
       | _ => raise Fail "StableGraph.assertIsRecvNode"

  fun assertIsSpawnNode node =
    case (getNodeEnv node) of
         ACTION {act = SPAWN_ACT _, ...} => ()
       | _ => raise Fail "StableGraph.assertIsSpawnNode"


  (* Called by the first thread spawned by runDML *)
  fun handleInit () : unit =
  let
    val nodeRef = S.tidNode ()
    val _ = case !nodeRef of
                 SOME _ => raise Fail "StableGraph.handleInit: tid already has a node"
               | NONE => ()
    val beginNode = G.newNode (depGraph)
    val act = ACTION {aid = newAid (), act = BEGIN_ACT}
    val _ = setNodeEnv (beginNode, act)
  in
    nodeRef := (SOME beginNode)
  end

  (* Must be called by spawning thread. Adds \po edge.
   * Returns: a new begin node.
   * *)
  fun handleSpawn {newTid : CML.thread_id} : unit =
  let
    val spawnNode = G.newNode (depGraph)
    val tidInt = CML.tidToInt newTid
    val childAct = ACTION_ID {aid = 0, tid = ThreadId tidInt, rid = 0, pid = ProcessId (!processId)}
    val _ = setNodeEnv (spawnNode, ACTION {aid = newAid (), act = SPAWN_ACT {childAct = childAct}})
    val _ = addProgramOrderEdge (spawnNode)
    val beginNode = G.newNode (depGraph)
    val _ = setNodeEnv (beginNode, ACTION {aid = childAct, act = BEGIN_ACT})
    val childNodeRef = CML.tidToNode newTid
    val _ = childNodeRef := (SOME beginNode)
  in
    ()
  end

  fun handleSend {cid: channel_id} =
  let
    val sendNode = G.newNode (depGraph)
    val aid = newAid ()
    val act = ACTION {aid = aid, act = SEND_ACT {cid = cid, matchAct = NONE}}
    val _ = setNodeEnv (sendNode, act)
    val _ = addProgramOrderEdge sendNode
  in
    aid
  end

  fun handleRecv {cid: channel_id} =
  let
    val recvNode = G.newNode (depGraph)
    val aid = newAid ()
    val act = ACTION {aid = aid, act = RECV_ACT {cid = cid, matchAct = NONE}}
    val _ = setNodeEnv (recvNode, act)
    val _ = addProgramOrderEdge recvNode
  in
    aid
  end

  fun setMatchAct (node: unit N.t, matchAct: action_id) =
  let
    val ACTION {aid, act} = getNodeEnv node
    val newAct = case act of
                      SEND_ACT {cid, matchAct = NONE} => SEND_ACT {cid = cid, matchAct = SOME matchAct}
                    | RECV_ACT {cid, matchAct = NONE} => RECV_ACT {cid = cid, matchAct = SOME matchAct}
                    | _ => raise Fail "StableGraph.setMatchAct(2)"
  in
    setNodeEnv (node, ACTION {aid = aid, act = newAct})
  end
end
