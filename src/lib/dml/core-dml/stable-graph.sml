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
   * Action Id
   *******************************************************************)

  val actionCounter = ref 0

  fun newActionNum () =
  let
    val id = !actionCounter
    val _ = actionCounter := id + 1
  in
    id
  end

  (********************************************************************
   * Action
   *******************************************************************)

  (* provides a unique action id across the program *)
  datatype action_id = ACTION_ID of {pid: process_id, tid: thread_id, aid: int}

  fun aidToString (ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, aid = aid}) =
    concat [Int.toString pid, ":", Int.toString tid, ":", Int.toString aid]

  fun newAid () =
  let
    val pid = !processId
    val tid = S.tidInt ()
    val aid = newActionNum ()
  in
    ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, aid = aid}
  end


  datatype action_type = SEND_ACT of {cid: channel_id, matchAct: action_id option}
                       | RECV_ACT of {cid: channel_id, matchAct: action_id option}
                       | SPAWN_ACT of {childAct: action_id}
                       | BEGIN_ACT

  fun actTypeToString at =
    case at of
         SEND_ACT {cid = ChannelId cstr, matchAct = NONE} => concat ["S (",cstr,",*)"]
       | SEND_ACT {cid = ChannelId cstr, matchAct = SOME act} => concat ["S (",cstr,",",aidToString act,")"]
       | RECV_ACT {cid = ChannelId cstr, matchAct = NONE} => concat ["R (",cstr,",*)"]
       | RECV_ACT {cid = ChannelId cstr, matchAct = SOME act} => concat ["R (",cstr,",",aidToString act,")"]
       | SPAWN_ACT {childAct} => concat ["F (", aidToString childAct, ")"]
       | BEGIN_ACT => "B"

  datatype action = DUMMY | ACTION of {aid: action_id, act: action_type}

  fun actionToString (ACTION {aid, act}) =
    concat ["[",aidToString aid,",",actTypeToString act,"]"]



  (********************************************************************
   * Node management
   *******************************************************************)

  val {get = getNodeEnv: unit N.t -> {cont: (unit -> unit) option, (* Continuation *)
                                      act: action},
       set = setNodeEnv, ...} =
        Property.getSet (N.plist, Property.initRaise ("StableGraph.nodeEnv", N.layout))

  val node2Cont = #cont o getNodeEnv
  val node2Act = #act o getNodeEnv

  fun getAid node =
    case node2Act node of
         ACTION {aid, ...} => aid
       | _ => raise Fail "StableGraph.getAid"


  fun nodeToString (node) =
    case getNodeEnv node of
         {cont = NONE, act} => concat ["{", actionToString act,",X}"]
       | {cont = SOME _, act} => concat ["{", actionToString act,",K}"]

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
         {act = DUMMY, cont} => raise Fail "StableGraph.assertIsBeginNode"
       | {act = ACTION {act = BEGIN_ACT, ...}, cont} => ()
       | _ => raise Fail "StableGraph.assertIsBeginNode"

  fun assertIsSendNode node =
    case (getNodeEnv node) of
         {act = DUMMY, cont} => raise Fail "StableGraph.assertIsSendNode"
       | {act = ACTION {act = SEND_ACT _, ...}, cont} => ()
       | _ => raise Fail "StableGraph.assertIsSendNode"

  fun assertIsRecvNode node =
    case (getNodeEnv node) of
         {act = DUMMY, cont} => raise Fail "StableGraph.assertIsRecvNode"
       | {act = ACTION {act = RECV_ACT _, ...}, cont} => ()
       | _ => raise Fail "StableGraph.assertIsRecvNode"

  fun assertIsSpawnNode node =
    case (getNodeEnv node) of
         {act = DUMMY, cont} => raise Fail "StableGraph.assertIsSpawnNode"
       | {act = ACTION {act = SPAWN_ACT _, ...}, cont} => ()
       | _ => raise Fail "StableGraph.assertIsSpawnNode"


  (* Must be called by the thread for which being node is being set *)
  fun handleBegin node : unit =
  let
    val _ = assertIsBeginNode node
    val nodeRef = S.tidNode ()
  in
      case !nodeRef of
         SOME _ => raise Fail "StableGraph.handleBegin: tid already has a node"
       | NONE => nodeRef := SOME node
  end

  (* Must be called by spawning thread. Adds \po edge.
   * Returns: a new begin node.
   * *)
  fun handleSpawn {newTid : int} : unit N.t =
  let
    val spawnNode = G.newNode (depGraph)
    val childAct = ACTION_ID {aid = newActionNum (), tid = ThreadId newTid, pid = ProcessId (!processId)}
    val _ = setNodeEnv (spawnNode, {cont = NONE, act = ACTION {aid = newAid (), act = SPAWN_ACT {childAct = childAct}}})
    val _ = addProgramOrderEdge (spawnNode)
    val beginNode = G.newNode (depGraph)
    val _ = setNodeEnv (beginNode, {cont = NONE, act = ACTION {aid = childAct, act = BEGIN_ACT}})
  in
    beginNode
  end

  fun handleSend {cid: channel_id} =
  let
    val sendNode = G.newNode (depGraph)
    val _ = setNodeEnv (sendNode, {cont = NONE, act = ACTION {aid = newAid (), act = SEND_ACT {cid = cid, matchAct = NONE}}})
  in
    addProgramOrderEdge sendNode
  end

  fun handleRecv {cid: channel_id} =
  let
    val recvNode = G.newNode (depGraph)
    val _ = setNodeEnv (recvNode, {cont = NONE, act = ACTION {aid = newAid (), act = RECV_ACT {cid = cid, matchAct = NONE}}})
  in
    addProgramOrderEdge recvNode
  end

  fun setMatchAct (node: unit N.t, matchAct: action_id) =
  let
    val cont = node2Cont node
    val {aid, act} = case node2Act node of
                          ACTION a => a
                        | DUMMY => raise Fail "StableGraph.setMatchAct(1)"
    val newAct = case act of
                      SEND_ACT {cid, matchAct = NONE} => SEND_ACT {cid = cid, matchAct = SOME matchAct}
                    | RECV_ACT {cid, matchAct = NONE} => RECV_ACT {cid = cid, matchAct = SOME matchAct}
                    | _ => raise Fail "StableGraph.setMatchAct(2)"
  in
    setNodeEnv (node, {cont = cont, act = ACTION {aid = aid, act = newAct}})
  end
end
