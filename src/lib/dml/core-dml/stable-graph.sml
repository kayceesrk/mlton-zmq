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

  val dummyAid = ACTION_ID {pid = ProcessId ~1, tid = ThreadId ~1, rid = ~1, aid = ~1}

  fun newAid () =
  let
    val pid = !processId
    val tid = S.tidInt ()
    val aid = S.tidNextActionNum ()
    val rid = S.tidRev ()
  in
    ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid = rid, aid = aid}
  end


  datatype action_type = SEND_WAIT of {cid: channel_id, matchAid: action_id option}
                       | SEND_ACT of {cid: channel_id}
                       | RECV_WAIT of {cid: channel_id, matchAid: action_id option}
                       | RECV_ACT of {cid: channel_id}
                       | SPAWN of {childTid: thread_id}
                       | BEGIN of {parentAid: action_id}

  fun aidToPidInt (ACTION_ID {pid = ProcessId pidInt, ...}) = pidInt
  fun aidToTidInt (ACTION_ID {tid = ThreadId tidInt, ...}) = tidInt

  fun actTypeToString at =
    case at of
         SEND_WAIT {cid = ChannelId cstr, matchAid = NONE} => concat ["SW (",cstr,",*)"]
       | SEND_WAIT {cid = ChannelId cstr, matchAid = SOME act} => concat ["SW (",cstr,",",aidToString act,")"]
       | SEND_ACT {cid = ChannelId cstr} => concat ["SA (", cstr, ")"]
       | RECV_WAIT {cid = ChannelId cstr, matchAid = NONE} => concat ["RW (",cstr,",*)"]
       | RECV_WAIT {cid = ChannelId cstr, matchAid = SOME act} => concat ["RW (",cstr,",",aidToString act,")"]
       | RECV_ACT {cid = ChannelId cstr} => concat ["RA (", cstr, ")"]
       | BEGIN {parentAid} => concat ["B (", aidToString parentAid, ")"]
       | SPAWN {childTid = ThreadId tid} => concat ["F(", Int.toString tid, ")"]

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
    val _ = G.addEdge (depGraph, {from = newNode, to = currentNode})
  in
    setCurrentNode (SOME newNode)
  end

  fun handleInit {parentAid : action_id} : unit =
  let
    val nodeRef = S.tidNode ()
    val _ = case !nodeRef of
                 SOME _ => raise Fail "StableGraph.handleInit: tid already has a node"
               | NONE => ()
    val beginNode = G.newNode (depGraph)
    val act = ACTION {aid = newAid (), act = BEGIN {parentAid = parentAid}}
    val _ = setNodeEnv (beginNode, act)
    val _ = nodeRef := (SOME beginNode)
    val _ = S.saveCont (fn () => handleInit {parentAid = parentAid})
  in
    ()
  end

  (* Must be called by spawning thread. Adds \po edge.
   * Returns: a new begin node.
   * *)
  fun handleSpawn {childTid} =
  let
    val spawnNode = G.newNode (depGraph)
    val spawnAid = newAid ()
    val _ = setNodeEnv (spawnNode, ACTION {aid = spawnAid, act = SPAWN {childTid = childTid}})
    val _ = addProgramOrderEdge (spawnNode)
  in
    spawnAid
  end

  fun handleSend {cid: channel_id} =
  let
    (* act *)
    val actNode = G.newNode (depGraph)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = SEND_ACT {cid = cid}}
    val _ = setNodeEnv (actNode, actAct)
    val _ = addProgramOrderEdge actNode
    (* wait *)
    val waitNode = G.newNode (depGraph)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = SEND_WAIT {cid = cid, matchAid = NONE}}
    val _ = setNodeEnv (waitNode, waitAct)
    val _ = addProgramOrderEdge waitNode
  in
    {waitNode = waitNode, actAid = actAid}
  end

  fun handleRecv {cid: channel_id} =
  let
    (* act *)
    val actNode = G.newNode (depGraph)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = RECV_ACT {cid = cid}}
    val _ = setNodeEnv (actNode, actAct)
    val _ = addProgramOrderEdge actNode
    (* wait *)
    val waitNode = G.newNode (depGraph)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = RECV_WAIT {cid = cid, matchAid = NONE}}
    val _ = setNodeEnv (waitNode, waitAct)
    val _ = addProgramOrderEdge waitNode
  in
    {waitNode = waitNode, actAid = actAid}
  end

  fun setMatchAct (node: unit N.t) (matchAid: action_id) =
  let
    val ACTION {aid, act} = getNodeEnv node
    val newAct = case act of
                      SEND_WAIT {cid, matchAid = NONE} => SEND_WAIT {cid = cid, matchAid = SOME matchAid}
                    | RECV_WAIT {cid, matchAid = NONE} => RECV_WAIT {cid = cid, matchAid = SOME matchAid}
                    | _ => raise Fail "StableGraph.setMatchAct(2)"
  in
    setNodeEnv (node, ACTION {aid = aid, act = newAct})
  end
end
