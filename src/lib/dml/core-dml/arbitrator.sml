(* arbitrator.sig
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)

structure Arbitrator : ARBITRATOR =
struct

  structure G = DirectedGraph
  structure N = G.Node

  open RepTypes
  open ActionManager
  open CommunicationManager

  val {get = act, set = setAct, ...} =
    Property.getSetOnce (N.plist, Property.initRaise ("NodeLocator.act", N.layout))

  val graph = G.new ()

  structure NodeLocator =
  struct
    val dict : unit N.t AISD.dict ref = ref (AISD.empty)

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

  fun processAdd {action, prevAction} =
    let
      val curNode = NL.node action
      val _ = case prevAction of
                    NONE => ()
                  | SOME prev => ignore (G.addEdge (graph, {from = NL.node prev, to = curNode}))
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


  fun startArbitrator {sink = sink_str, source = source_str} =
  let
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

    fun mainLoop () =
      case msgRecvSafe () of
           NONE => mainLoop ()
         | SOME msg =>
             let
               val _ =
                 case msg of
                     AR_ADD m => processAdd m
                   | _ => ()
             in
               mainLoop ()
             end
  in
    ignore (RunCML.doit (mainLoop, NONE))
  end
end
