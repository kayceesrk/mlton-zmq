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
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  open RepTypes
  open ActionManager
  open CommunicationManager


  val {get = act, set = setAct, ...} =
    Property.getSetOnce (N.plist, Property.initRaise ("NodeLocator.act", N.layout))

  val graph = G.new ()

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  structure NodeLocator =
  struct
    val dict : N.t AISD.dict ref = ref (AISD.empty)

    fun node (action as ACTION {aid, act}) =
    let
      fun insertAndGetNewNode () =
      let
        val n = G.newNode graph
        val _ = setAct (n, action)
        val _ = dict := AISD.insert (!dict) aid n
      in
        n
      end
    in
      case AISD.find (!dict) aid of
           SOME n => n
         | NONE => insertAndGetNewNode ()
    end
  end

  structure NL = NodeLocator

  exception Cycle

  fun processCommit action =
  let
    val startNode = NL.node action
    val {get = amVisiting, set = setVisiting, destroy, ...} =
      Property.destGetSet (N.plist, Property.initFun (fn _ => ref false))
    val w = {startNode = fn n => amVisiting n := true,
             finishNode = fn n =>
               let
                 val _ = amVisiting n := false
                 val _ = ListMLton.map (N.successors (graph, n),
                      fn e => G.removeEdge (graph, {from = n, to = E.to (graph, e)}))
               in
                 ()
               end,
             handleTreeEdge = D.ignore,
             handleNonTreeEdge = fn e => if !(amVisiting (E.to (graph, e))) then raise Cycle else (),
             startTree = D.ignore,
             finishTree = D.ignore,
             finishDfs = destroy}
    val _ = G.dfsNodes (graph, [startNode], w) handle Cycle => (destroy (); raise Cycle)
  in
    ()
  end

  fun processAdd {action, prevAction} =
    let
      val curNode = NL.node action
      val _ = case prevAction of
                    NONE => ()
                  | SOME prev => ignore (G.addEdge (graph, {to = NL.node prev, from = curNode}))
      val ACTION {act, aid} = action
    in
      case act of
           BEGIN {parentAid} =>
              let
                val spawnAct = ACTION {aid = parentAid, act = SPAWN {childTid = aidToTid aid}}
              in
                ignore (G.addEdge (graph, {from = curNode, to = NL.node spawnAct}))
              end
         | SEND_WAIT {cid, matchAid = SOME matchAid} =>
             let
               val recvAct = ACTION {aid = matchAid, act = RECV_ACT {cid = cid}}
             in
               ignore (G.addEdge (graph, {from = curNode, to = NL.node recvAct}))
             end
         | RECV_WAIT {cid, matchAid = SOME matchAid} =>
             let
               val sendAct = ACTION {aid = matchAid, act = SEND_ACT {cid = cid}}
             in
               ignore (G.addEdge (graph, {from = curNode, to = NL.node sendAct}))
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
           NONE => mainLoop ()
         | SOME msg =>
             let
               val _ =
                 case msg of
                     AR_REQ_ADD m => processAdd m
                   | AR_REQ_COM {action} => processCommit action
                   | _ => ()
             in
               mainLoop ()
             end
  in
    ignore (RunCML.doit (mainLoop, NONE))
  end
end
