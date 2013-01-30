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
   * Debug
   *******************************************************************)

  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)


  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

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

  fun dummyAid () = ACTION_ID {pid = ProcessId (!processId), tid = ThreadId ~1, rid = ~1, aid = ~1}

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
                       | COM_RB (* This indicates the node that is inserted after commit or rollback *)

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
       | COM_RB => "CR"

  datatype action = ACTION of {aid: action_id, act: action_type}

  fun actionToString (ACTION {aid, act}) = concat ["[",aidToString aid,",",actTypeToString act,"]"]

  fun getSuccActForDFS act =
    case act of
         SEND_WAIT {matchAid = SOME aid, ...} => SOME aid
       | RECV_WAIT {matchAid = SOME aid, ...} => SOME aid
       | BEGIN {parentAid} => SOME parentAid
       | _ => NONE

  structure ActionIdOrdered
    :> ORDERED where type t = action_id
  = struct
    type t = action_id

    fun eq (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, aid = aid1},
            ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, aid = aid2}) =
            pid1 = pid2 andalso tid1 = tid2 andalso rid1 = rid2 andalso aid1 = aid2

    fun compare (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, aid = aid1},
                 ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, aid = aid2}) =
      (case Int.compare (pid1, pid2) of
           EQUAL => (case Int.compare (tid1, tid2) of
                          EQUAL => (case Int.compare (rid1, rid2) of
                                         EQUAL => Int.compare (aid1, aid2)
                                       | lg => lg)
                        | lg => lg)
         | lg => lg)
  end

  structure ActionIdSplaySet = SplaySet (structure Elem = ActionIdOrdered)
  structure AISS = ActionIdSplaySet
  structure ActionIdSplayDict = SplayDict (structure Key = ActionIdOrdered)
  structure AISD = ActionIdSplayDict

  (********************************************************************
   * Global tid
   *******************************************************************)

   datatype global_tid = GLOBAL_TID of {pid: process_id, tid: thread_id}

   fun aidToGid (ACTION_ID {pid, tid, ...}) = GLOBAL_TID {pid = pid, tid = tid}

   structure GIDOrdered
     :> ORDERED where type t = global_tid
   = struct
     type t = global_tid

     fun eq (GLOBAL_TID {pid = ProcessId pid1, tid = ThreadId tid1},
             GLOBAL_TID {pid = ProcessId pid2, tid = ThreadId tid2}) =
       pid1 = pid2 andalso tid1 = tid2

     fun compare (GLOBAL_TID {pid = ProcessId pid1, tid = ThreadId tid1},
                  GLOBAL_TID {pid = ProcessId pid2, tid = ThreadId tid2}) =
       (case Int.compare (pid1, pid2) of
             EQUAL => Int.compare (tid1, tid2)
           | lq => lq)
   end

  structure GlobalIdSplayDict = SplayDict (structure Key = GIDOrdered)
  structure GISD = GlobalIdSplayDict

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

  fun getAidFromTid tid =
    case !(CML.tidToNode tid) of
         NONE => raise Fail "StableGraph.getAidFromTid"
       | SOME n => (case getNodeEnv n of ACTION {aid, ...} => aid)


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

  fun handleInit {parentAid : action_id} =
  let
    val nodeRef = S.tidNode ()
    val _ = case !nodeRef of
                 SOME _ => raise Fail "StableGraph.handleInit: tid already has a node"
               | NONE => ()
    val beginNode = G.newNode (depGraph)
    val beginAid = newAid ()
    val act = ACTION {aid = beginAid, act = BEGIN {parentAid = parentAid}}
    val _ = setNodeEnv (beginNode, act)
    val _ = nodeRef := (SOME beginNode)
  in
    beginAid
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

  fun insertCommitRollbackNode () =
  let
    val nodeRef = S.tidNode ()
    val _ = case !nodeRef of
                 SOME _ => raise Fail "StableGraph.insertCommitRollbackNode : tid already has a node"
               | NONE => ()
    val crNode = G.newNode (depGraph)
    val comRbAid = newAid ()
    val act = ACTION {aid = comRbAid, act = COM_RB}
    val _ = setNodeEnv (crNode, act)
    val _ = nodeRef := (SOME crNode)
  in
    comRbAid
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

  fun setMatchAid (node: unit N.t) (matchAid: action_id) =
  let
    val ACTION {aid, act} = getNodeEnv node
    val newAct = case act of
                      SEND_WAIT {cid, matchAid = NONE} => SEND_WAIT {cid = cid, matchAid = SOME matchAid}
                    | RECV_WAIT {cid, matchAid = NONE} => RECV_WAIT {cid = cid, matchAid = SOME matchAid}
                    | _ => raise Fail "StableGraph.setMatchAid"
  in
    setNodeEnv (node, ACTION {aid = aid, act = newAct})
  end

  fun removeMatchAid (node: unit N.t) =
  let
    val ACTION {aid, act} = getNodeEnv node
    val newAct = case act of
                      SEND_WAIT {cid, ...} => SEND_WAIT {cid = cid, matchAid = NONE}
                    | RECV_WAIT {cid, ...} => RECV_WAIT {cid = cid, matchAid = NONE}
                    | _ => raise Fail "StableGraph.removeMatchAid"
  in
    setNodeEnv (node, ACTION {aid = aid, act = newAct})
  end

  fun getMatchAid (node: unit N.t) =
  let
    val ACTION {act, ...} = getNodeEnv node
    val matchAid = case act of
                      SEND_WAIT {matchAid = SOME mAid, ...} => mAid
                    | RECV_WAIT {matchAid = SOME mAid, ...} => mAid
                    | _ => raise Fail "StableGraph.getMatchAid"
  in
    matchAid
  end

  fun isAidLocal (ACTION_ID {pid = ProcessId pidInt, ...}) =
    pidInt = (!processId)

  fun getPrevAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid - 1}

  fun getNextAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid + 1}

  (********************************************************************
   * DFS
   *******************************************************************)

  fun aidToNode (aid as ACTION_ID {pid = ProcessId pid, tid, ...},
                 tid2tid) =
    if not (pid = !processId) then NONE
    else
      case tid2tid tid of
           NONE => NONE
         | SOME tid =>
              let
                val nodeRef = CML.tidToNode tid
                fun loop node =
                let
                  val ACTION {aid = nodeAid, ...} = getNodeEnv node
                in
                  if nodeAid = aid then SOME node
                  else (case N.successors node of
                            [] => NONE
                          | e::_ => loop (E.to e))
                end
              in
                loop (valOf (!nodeRef))
              end

  (* tid2tid : Converts from Dml's RepTypes.thread_id to CML.thread_id
   * hasBeenVisited : returns true if the node has been seen before, else
   * returns false and sets the node to be visited such that subsequent
   * queries with the same node return true. *)
  fun dfs {startNode : unit N.t,
           foo : unit N.t * 'a -> 'a,
           acc : 'a,
           tid2tid,
           hasBeenVisited : unit N.t -> bool} =
  let
      fun dfs'(n, acc) =
        if (hasBeenVisited n) then acc
        else
          let
            val _ = debug (fn () => "StableGraph.dfs: "^(nodeToString n))
            val newAcc = foo (n, acc)
            val adjs = N.successors n
            val succs = map E.to adjs
            val ACTION {act, ...} = getNodeEnv n
            val succs = case getSuccActForDFS act of
                            NONE => succs
                          | SOME aid =>
                              (case aidToNode (aid, tid2tid) of
                                    NONE => succs
                                  | SOME n' => n'::succs)
            val newAcc = ListMLton.fold (succs, newAcc, dfs')
          in
            newAcc
          end
      val ret = dfs' (startNode, acc)
  in
    ret
  end

  (********************************************************************
   * Rollback Helper + Stuff
   *******************************************************************)

  fun rhNodeToThreads {startNode       : unit N.t,
                       tid2tid         : thread_id -> CML.thread_id option,
                       visitedSet      : AISS.set} :
                      {localRestore    : AISS.set,
                       remoteRollbacks : action_id list,
                       visitedSet      : AISS.set} =
  let
    val visitedSet = ref visitedSet
    val myPID = !processId
    (* rollback dictionary: key: global_id, value: action_id *)
    val rbDict =
      let
        val ACTION {aid = startAid, ...} = getNodeEnv startNode
        val startGid = aidToGid startAid
      in
        GISD.singleton startGid startAid
      end

    fun hasBeenVisited node =
    let
      val ACTION {aid, ...} = getNodeEnv node
    in
      if AISS.member (!visitedSet) aid then true
      else
        (visitedSet := AISS.insert (!visitedSet) aid;
         false)
    end

    fun foo (node, rbDict) =
    let
      val ACTION {act, ...} = getNodeEnv node

      fun rbDictHandler newAid =
      let
        val gid = aidToGid newAid
      in
        (case GISD.find rbDict gid of
             NONE => GISD.insert rbDict gid newAid
           | SOME oldAid => (case ActionIdOrdered.compare (oldAid, newAid) of
                                 LESS => GISD.insert rbDict gid newAid
                               | _ => rbDict))
      end
    in
      case act of
           SEND_WAIT {matchAid = SOME newAid, ...} => rbDictHandler newAid
         | RECV_WAIT {matchAid = SOME newAid, ...} => rbDictHandler newAid
         | SPAWN {childTid = ThreadId tid} => rbDictHandler (ACTION_ID {pid = ProcessId (!processId), tid = ThreadId tid, rid = 0, aid = 0})
         | _ => rbDict
    end

    val rbDict =
      dfs {startNode = startNode, foo = foo, acc = rbDict,
           tid2tid = tid2tid, hasBeenVisited = hasBeenVisited}

    val (_,rbList) = ListMLton.unzip (GISD.toList rbDict)
    val (localRestore, remoteRollbacks) =
      ListMLton.fold (rbList, ([], []), fn (aid as ACTION_ID {pid = ProcessId pid, ...}, (l,r)) =>
                                          if pid = myPID then (aid::l, r)
                                          else (l, aid::r))
    val localRestore = ListMLton.fold (localRestore, AISS.empty, fn (aid, set) => AISS.insert set aid)
  in
    {localRestore = localRestore,
     remoteRollbacks = remoteRollbacks,
     visitedSet = !visitedSet}
  end

  fun getAidFromNode node =
    let
      val ACTION {aid, ...} = getNodeEnv node
    in
      aid
    end

  fun saveCont f =
  let
    val _ = debug (fn () => "StableGraph.saveCont")
  in
    S.saveCont (f)
  end

  fun restoreCont () =
  let
    val _ = debug (fn () => "StableGraph.restoreCont")
  in
    S.restoreCont ()
  end
end
