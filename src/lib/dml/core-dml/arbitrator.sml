(* arbitrator.sig
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)

structure Arbitrator : ARBITRATOR =
struct

  structure G = DirectedSubGraph
  structure N = G.Node
  structure E = G.Edge
  structure S = CML.Scheduler
  structure D = G.DfsParam
  structure ISS = IntSplaySet

  open RepTypes
  open ActionManager
  open CommunicationManager

  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)


  val {get = nodeGetAct, set = nodeSetAct, ...} =
    Property.getSetOnce (N.plist, Property.initRaise ("NodeLocator.act", N.layout))

  val {get = mustRollbackOnVisit, ...} =
    Property.getSetOnce (N.plist, Property.initFun (fn _ => ref false))

  val {get = isCommitted, ...} =
    Property.getSetOnce (N.plist, Property.initFun (fn _ => ref false))

  val graph = G.new ()

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  structure NodeLocator =
  struct
    val dict : N.t AidDict.dict ref = ref (AidDict.empty)

    fun node (action as ACTION {aid, act}) =
    let
      fun insertAndGetNewNode () =
      let
        val n = G.newNode graph
        val _ = nodeSetAct (n, action)
        val _ = dict := AidDict.insert (!dict) aid n
      in
        n
      end
    in
      case AidDict.find (!dict) aid of
           SOME n => n
         | NONE => insertAndGetNewNode ()
    end
  end
  structure NL = NodeLocator

  structure NodeWaiter =
  struct
    val dict : unit S.thread list AidDict.dict ref = ref (AidDict.empty)

    fun numSuccessors node =
      length (N.successors (graph, node))

    fun numSuccessorsExpected node =
    let
      val ACTION {act, ...} = nodeGetAct node
    in
      case act of
           SEND_WAIT _ => 2
         | SEND_ACT _ => 1
         | RECV_WAIT _ => 2
         | RECV_ACT _ => 1
         | SPAWN _ => 1
         | BEGIN _ => 1 (* parent*)
         | COM => 0
         | RB => 0
    end

    fun waitTillSated node =
      if numSuccessors node < numSuccessorsExpected node then
        let
          val _ = S.atomicSwitchToNext (fn t =>
            let
              val ACTION {aid, ...} = nodeGetAct node
              val _ = debug (fn () => "NodeWaiter.waitTillSated: waiting on "^(aidToString aid))
              fun merge l = t::l
            in
              dict := AidDict.insertMerge (!dict) aid [t] merge
            end)
          val _ = debug (fn () => "NodeWaiter.waitTillSated: resuming")
          val _ = S.atomicBegin ()
        in
          waitTillSated node
        end
      else ()

    fun resumeThreads node =
    let
      val ACTION {aid, ...} = nodeGetAct node
      val threadList = AidDict.lookup (!dict) aid
      val _ = dict := AidDict.remove (!dict) aid
      val _ = ListMLton.map (threadList, fn t => S.ready (S.prep t))
    in
      ()
    end handle AidDict.Absent => ()
  end
  structure NW = NodeWaiter


  fun processCommit {action, pushResult} =
  let
    val _ = S.atomicBegin ()
    val _ = debug (fn () => "Arbitrator.processCommit: "^(actionToString action))
    val maxVisit = ref (PTRDict.empty)
    val foundCycle = ref false
    val startNode = NL.node action
    val visitedNodes = ref []

    val {get = amVisiting, destroy, ...} =
      Property.destGetSet (N.plist, Property.initFun (fn _ => ref false))

    val ACTION {aid = actAid, ...} = action

    val w = {startNode = fn n =>
               let
                 val _ = if not (!(isCommitted n))
                         then NW.waitTillSated n
                         else ()
               in
                if !(mustRollbackOnVisit n) then
                  foundCycle := true
                else
                  (ListMLton.push (visitedNodes, n);
                   amVisiting n := true)
               end,
             finishNode = fn n =>
               let
                 val _ = amVisiting n := false

                 (* Insert into maxVisit *)
                 val ACTION{aid, ...} = nodeGetAct n
                 val ptrId = aidToPtr aid
                 val actNum = aidToActNum aid
                 fun insert () = maxVisit := PTRDict.insert (!maxVisit) ptrId actNum
                 val _ = case PTRDict.find (!maxVisit) ptrId of
                              NONE => insert ()
                            | SOME actNum' => if actNum > actNum' then insert () else ()

                 (* Remove outoing edges *)
                 val _ = ListMLton.map (N.successors (graph, n),
                      fn e => G.removeEdge (graph, {from = n, to = E.to (graph, e)}))
               in
                 ()
               end,
             handleTreeEdge = D.ignore,
             handleNonTreeEdge = fn e => if !(amVisiting (E.to (graph, e))) then
                                           foundCycle := true
                                         else (),
             startTree = D.ignore,
             finishTree = D.ignore,
             finishDfs = destroy}

    val _ = G.dfsNodes (graph, [startNode], w)
    val res =
      if !foundCycle then
        let
          val _ = ignore (ListMLton.map (!visitedNodes, fn n => mustRollbackOnVisit n := true));
          val res = AR_RES_FAIL {rollbackAids = !maxVisit, dfsStartAct = action}
          val _ = msgSend res
        in
          res
        end
      else
        let
          val _ = ignore (ListMLton.map (!visitedNodes, fn n => isCommitted n := true));
          val res = AR_RES_SUCC {dfsStartAct = action}
          val _ = msgSend res
        in
          res
        end
    val _ = S.atomicEnd ()
    val _ = pushResult res
  in
    ()
  end

  fun markCycleDepGraph dfsStartAct =
  let
    val _ = S.atomicBegin ()
    val _ = debug (fn () => concat ["Arbitrator.markCycleDepGraph: ", actionToString dfsStartAct])
    val _ = Assert.assertAtomic' ("Arbitrator.markCycleDepGraph", NONE)
    val startNode = NL.node dfsStartAct
    val w = {startNode = fn n =>
              let
                val isCommitted = !(isCommitted n)
                val isMarkedAsRolledBack = !(mustRollbackOnVisit n)
                val _ = if not (isCommitted orelse isMarkedAsRolledBack)
                        then NW.waitTillSated n
                        else ()
              in
                (mustRollbackOnVisit n) := true
              end,
             finishNode = fn n =>
               ignore (ListMLton.map (N.successors (graph, n),
                 fn e => G.removeEdge (graph, {from = n, to = E.to (graph, e)}))),
             handleTreeEdge = D.ignore, handleNonTreeEdge = D.ignore,
             startTree = D.ignore, finishTree = D.ignore, finishDfs = fn () => ()}
    val _ = G.dfsNodes (graph, [startNode], w)
    val _ = S.atomicEnd ()
  in
    ()
  end



  fun processAdd {action, prevAction} =
    let
      fun addEdge {from, to} =
      let
        val _ = ignore (G.addEdge (graph, {to = to, from = from}))
      in
        NW.resumeThreads from
      end

      val _ = debug (fn () => "Arbitrator.processAdd: action="^(actionToString action)^" prevAction="^
                   (case prevAction of NONE => "NONE" | SOME a => actionToString a))
      val curNode = NL.node action
      val _ = case prevAction of
                    NONE => ()
                  | SOME prev => addEdge {to = NL.node prev, from = curNode}
      val ACTION {act, aid} = action
    in
      case act of
           BEGIN {parentAid} =>
              let
                val spawnAct = ACTION {aid = parentAid, act = SPAWN {childTid = aidToTid aid}}
              in
                addEdge {from = curNode, to = NL.node spawnAct}
              end
         | SEND_WAIT {cid, matchAid = SOME matchAid} =>
             let
               val recvAct = ACTION {aid = matchAid, act = RECV_ACT {cid = cid}}
             in
               addEdge {from = curNode, to = NL.node recvAct}
             end
         | RECV_WAIT {cid, matchAid = SOME matchAid} =>
             let
               val sendAct = ACTION {aid = matchAid, act = SEND_ACT {cid = cid}}
             in
               addEdge {from = curNode, to = NL.node sendAct}
             end
         | _ => ()
    end
end
