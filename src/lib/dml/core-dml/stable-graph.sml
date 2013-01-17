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
                       | SEND_WAIT of {cid: channel_id}
                       | RECV_ACT of {cid: channel_id, matchAct: action_id option}
                       | RECV_WAIT of {cid: channel_id}
                       | SPAWN of {childAct: action_id}
                       | BEGIN

  fun aidToPidInt (ACTION_ID {pid = ProcessId pidInt, ...}) = pidInt
  fun aidToTidInt (ACTION_ID {tid = ThreadId tidInt, ...}) = tidInt

  fun actTypeToString at =
    case at of
         SEND_ACT {cid = ChannelId cstr, matchAct = NONE} => concat ["SA (",cstr,",*)"]
       | SEND_ACT {cid = ChannelId cstr, matchAct = SOME act} => concat ["SA (",cstr,",",aidToString act,")"]
       | SEND_WAIT {cid = ChannelId cstr} => concat ["SW (", cstr, ")"]
       | RECV_ACT {cid = ChannelId cstr, matchAct = NONE} => concat ["RA (",cstr,",*)"]
       | RECV_ACT {cid = ChannelId cstr, matchAct = SOME act} => concat ["RA (",cstr,",",aidToString act,")"]
       | RECV_WAIT {cid = ChannelId cstr} => concat ["RW (", cstr, ")"]
       | SPAWN {childAct} => concat ["F (", aidToString childAct, ")"]
       | BEGIN => "B"

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

  (* Called by the first thread spawned by runDML *)
  fun handleInit () : unit =
  let
    val nodeRef = S.tidNode ()
    val _ = case !nodeRef of
                 SOME _ => raise Fail "StableGraph.handleInit: tid already has a node"
               | NONE => ()
    val beginNode = G.newNode (depGraph)
    val act = ACTION {aid = newAid (), act = BEGIN}
    val _ = setNodeEnv (beginNode, act)
    val _ = nodeRef := (SOME beginNode)
    val _ = (S.tidRoot ()) := (SOME beginNode)
  in
    ()
  end

  (* Must be called by spawning thread. Adds \po edge.
   * Returns: a new begin node.
   * *)
  fun handleSpawn {newTid : CML.thread_id} : unit =
  let
    val spawnNode = G.newNode (depGraph)
    val tidInt = CML.tidToInt newTid
    val childAct = ACTION_ID {aid = 0, tid = ThreadId tidInt, rid = 0, pid = ProcessId (!processId)}
    val _ = setNodeEnv (spawnNode, ACTION {aid = newAid (), act = SPAWN {childAct = childAct}})
    val _ = addProgramOrderEdge (spawnNode)

    val beginNode = G.newNode (depGraph)
    val _ = setNodeEnv (beginNode, ACTION {aid = childAct, act = BEGIN})
    val _ = (CML.tidToNode newTid) := (SOME beginNode)
    val _ = (CML.tidToRoot newTid) := (SOME beginNode)
  in
    ()
  end

  fun handleSend {cid: channel_id} =
  let
    (* act *)
    val actNode = G.newNode (depGraph)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = SEND_ACT {cid = cid, matchAct = NONE}}
    val _ = setNodeEnv (actNode, actAct)
    val _ = addProgramOrderEdge actNode
    (* wait *)
    val waitNode = G.newNode (depGraph)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = SEND_WAIT {cid = cid}}
    val _ = setNodeEnv (waitNode, waitAct)
    val _ = addProgramOrderEdge waitNode
  in
    {waitAid = waitAid, actNode = actNode}
  end

  fun handleRecv {cid: channel_id} =
  let
    (* act *)
    val actNode = G.newNode (depGraph)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = RECV_ACT {cid = cid, matchAct = NONE}}
    val _ = setNodeEnv (actNode, actAct)
    val _ = addProgramOrderEdge actNode
    (* wait *)
    val waitNode = G.newNode (depGraph)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = RECV_WAIT {cid = cid}}
    val _ = setNodeEnv (waitNode, waitAct)
    val _ = addProgramOrderEdge waitNode
  in
    {waitAid = waitAid, actNode = actNode}
  end

  fun setMatchAct (node: unit N.t) (matchAct: action_id) =
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
