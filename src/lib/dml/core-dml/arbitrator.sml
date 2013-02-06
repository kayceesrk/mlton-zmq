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


  fun processCommit action =
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
               (NW.waitTillSated n;
               if !(mustRollbackOnVisit n) then
                 foundCycle := true
               else
                 (ListMLton.push (visitedNodes, n);
                  amVisiting n := true)),
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
    val _ = if !foundCycle then
              (ignore (ListMLton.map (!visitedNodes, fn n => mustRollbackOnVisit n := true));
               msgSend (AR_RES_FAIL {rollbackAids = !maxVisit}))
            else
              msgSend (AR_RES_SUCC {aid = actAid})
  in
    S.atomicEnd ()
  end

  fun processAdd {action, prevAction} =
    let
      fun addEdge {from, to} =
      let
        val _ = ignore (G.addEdge (graph, {to = to, from = from}))
      in
        NW.resumeThreads from
      end

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


  fun startArbitrator {sink = sink_str, source = source_str, numPeers} =
  let
    (* State for join and exit*)
    val peers = ref (ISS.empty)
    val exitDaemon = ref false

    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
    val _ = ZMQ.sockSetSubscribe (source, Vector.tabulate (0, fn _ => 0wx0))
    (* In order to allow arbitrator to receive messages. Make sure processIds
     * of clients is >= 0. *)
    val _ = processId := ~1
    val _ = proxy := PROXY {context = SOME context, source = SOME source, sink = SOME sink}

    val _ = debug' ("DmlDecentralized.connect.join(1)")
    fun join n =
    let
      val n = if n = 100000 then
                let
                  val _ = debug' ("DmlDecentralized.connect.join: send CONN")
                  val _ = msgSendSafe (CONN {pid = ProcessId (!processId)})
                in
                  0
                end
              else n+1
      val () = case msgRecvSafe () of
                    NONE => join n
                  | SOME (CONN {pid = ProcessId pidInt}) =>
                      let
                        val _ = debug' ("DmlDecentralized.connect.join(2)")
                        val _ = if ISS.member (!peers) pidInt then ()
                                else peers := ISS.insert (!peers) pidInt
                      in
                        if ISS.size (!peers) = numPeers then msgSendSafe (CONN {pid = ProcessId (!processId)})
                        else join n
                      end
                  | SOME m => raise Fail ("DmlDecentralized.connect: unexpected message during connect" ^ (msgToString m))
    in
      ()
    end
    val _ = join 0

    fun mainLoop () =
      case msgRecvSafe () of
           NONE => (CML.yield (); mainLoop ())
         | SOME msg =>
             let
               val _ =
                 case msg of
                     AR_REQ_ADD m => (S.atomicBegin (); processAdd m; S.atomicEnd ())
                   | AR_REQ_COM {action} => ignore (CML.spawn (fn () => processCommit action))
                   | _ => ()
             in
               mainLoop ()
             end
  in
    ignore (RunCML.doit (mainLoop, NONE))
  end
end
