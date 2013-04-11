(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure CycleDetector : CYCLE_DETECTOR =
struct

  structure G = DirectedSubGraph
  structure N = G.Node
  structure E = G.Edge
  structure S = CML.Scheduler
  structure D = G.DfsParam

  open RepTypes
  open ActionHelper
  open CommunicationHelper

  (* structure Assert = LocalAssert(val assert = true) *)
  structure Debug = LocalDebug(val debug = false)


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

    fun node (EVENT _) = raise Fail "NodeLocator.node: saw event"
      | node (action as ACTION {aid, ...}) =
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

    fun findNode aid =
      AidDict.find (!dict) aid

    (* XXX Cleanup *)
  end
  structure NL = NodeLocator

  structure NodeWaiter =
  struct
    val dict : unit S.thread list AidDict.dict ref = ref (AidDict.empty)

    fun numSuccessors node =
      length (N.successors (graph, node))

    fun numSuccessorsExpected node =
    let
      val act =
        case nodeGetAct node of
             ACTION {act, ...} => act
           | EVENT _ => raise Fail "NodeWaiter.numSuccessorsExpected: saw event"
    in
      case act of
           SEND_WAIT _ => 2 (* parent, match *)
         | SEND_ACT _ => 1  (* parent *)
         | RECV_WAIT _ => 2 (* parent, match *)
         | RECV_ACT _ => 1  (* parent *)
         | SPAWN _ => 1     (* parent *)
         | BEGIN _ => 1     (* parent *)
         | NOOP => 1        (* parent *)
         | COM => 0
         | RB => 0
    end

    fun waitTillSated node =
      if numSuccessors node < numSuccessorsExpected node andalso
         not ((!(isCommitted node)) orelse (!(mustRollbackOnVisit node))) then
        let
          val _ = S.atomicSwitchToNext (fn t =>
            let
              val aid = actionToAid (nodeGetAct node)
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
      val aid = actionToAid (nodeGetAct node)
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
    val _ = debug (fn () => "CycleDetector.processCommit: "^(actionToString action))
    val foundCycle = ref false
    val startNode = NL.node action

    (* For log-based rollback recovery *)
    val unsafeActions = ref AidSet.empty
    val stack = ref []

    fun handleCycle () =
    let
      val _ = foundCycle := true
      fun foo (n, acc) =
      let
        val _ = debug' ("handleCycle: n="^(aidToString (actionToAid (nodeGetAct n))))
        val _ = mustRollbackOnVisit n := true
      in
        AidSet.insert acc (actionToAid (nodeGetAct n))
      end
    in
      unsafeActions := ListMLton.fold (!stack, !unsafeActions, foo)
    end

    val {get = amVisiting, destroy, ...} =
      Property.destGetSet (N.plist, Property.initFun (fn _ => ref false))

    val w = {startNode = fn n =>
               let
                 val _ = debug' ("processCommit.startNode: "^(actionToString (nodeGetAct n)))
                 val committed = !(isCommitted n)
                 val isMarkedAsRolledBack = !(mustRollbackOnVisit n)
                 val _ = if not (committed orelse isMarkedAsRolledBack)
                         then NW.waitTillSated n
                         else ()
               in
                if !(mustRollbackOnVisit n) then
                  handleCycle ()
                else
                  (isCommitted n := true;
                   ListMLton.push (stack, n);
                   amVisiting n := true)
               end,
             finishNode = fn n =>
               let
                 val _ = if MLton.equal (hd(!stack), n) then ignore (ListMLton.pop stack) else ()
                 val _ = amVisiting n := false

                 (* Remove outoing edges *)
                 val _ = ListMLton.map (N.successors (graph, n),
                      fn e => G.removeEdge (graph, {from = n, to = E.to (graph, e)}))
               in
                 ()
               end,
             handleNonTreeEdge =
              fn e => if !(amVisiting (E.to (graph, e))) orelse
                         AidSet.member (!unsafeActions) (actionToAid (nodeGetAct (E.to (graph, e)))) then
                        handleCycle ()
                      else (),
             handleTreeEdge = D.ignore,
             startTree = D.ignore,
             finishTree = D.ignore,
             finishDfs = destroy}

    val _ = G.dfsNodes (graph, [startNode], w)
    val res =
      if !foundCycle then
        let
          val _ = AidSet.app (fn aid => debug' ("unsafeAid: "^(aidToString aid))) (!unsafeActions)
          val rollbackAids = AidSet.foldr (fn (aid, acc) =>
            let
              fun merge anum = if (aidToActNum aid) < anum then (aidToActNum aid) else anum
            in
              PTRDict.insertMerge acc (aidToPtr aid) (aidToActNum aid) merge
            end) PTRDict.empty (!unsafeActions)
          val res = AR_RES_FAIL {rollbackAids = rollbackAids, dfsStartAct = action}
          val _ = msgSend res
          (* val _ = Assert.assert ([], fn () => "done!", fn _ => false) *)
        in
          res
        end
      else
        let
          val res = AR_RES_SUCC {dfsStartAct = action}
          val _ = msgSend res
        in
          res
        end
    val _ = S.atomicEnd ()
    val _ = pushResult res
  in
    ()
  end handle Empty => (debug' ("processCommit raised exception"); S.atomicEnd ())

  fun processAdd {action, prevAction} =
    let
      fun addEdge {from, to} =
      let
        val _ = ignore (G.addEdge (graph, {to = to, from = from}))
      in
        NW.resumeThreads from
      end

      val _ = debug (fn () => "CycleDetector.processAdd: action="^(actionToString action)^" prevAction="^
                   (case prevAction of NONE => "NONE" | SOME a => actionToString a))
      val curNode = NL.node action
      val _ = case prevAction of
                    NONE => ()
                  | SOME prev => addEdge {to = NL.node prev, from = curNode}
      val {act, aid} = case action of
                            ACTION m => m
                          | EVENT _ => raise Fail "CycleDetector.processAdd: saw event"
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

  fun isMatched actAid =
  let
    val waitAid = getNextAid actAid
  in
    case NL.findNode waitAid of
         NONE => false
       | SOME n => NW.numSuccessorsExpected n <= NW.numSuccessors n
  end
end
