(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure DmlDecentralized : DML =
struct
  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)

  structure ZMQ = MLton.ZMQ
  structure IntDict = IntSplayDict
  structure StrDict = StringSplayDict
  structure S = CML.Scheduler
  structure C = CML
  structure IQ = IQueue
  structure ISS = IntSplaySet
  structure Weak = MLton.Weak

  open RepTypes
  open StableGraph

  (* -------------------------------------------------------------------- *)
  (* Datatype definitions *)
  (* -------------------------------------------------------------------- *)


  datatype content = S_ACT  of {aid: action_id, value: w8vec}
                   | R_ACT  of {aid: action_id}
                   | S_JOIN of {aid: action_id}
                   | R_JOIN of {aid: action_id}
                   | J_REQ
                   | J_ACK


  datatype msg = MSG of {cid : channel_id,
                         pid : process_id,
                         tid : thread_id,
                         cnt : content}

  datatype 'a chan = CHANNEL of channel_id

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})

  val blockedThreads = ref (IntDict.empty)
  (* Key: string (Channel), Value: {sendQ: {aid: action_id, value: w8vec} , recvQ: {aid: action_id}} *)
  val unmatchedActs = ref (StrDict.empty)
  (* Key: action_id (matched), Value: node (waitNode) *)
  val matchedActs = ref (AISD.empty)
  val localMsgQ : msg IQ.iqueue = IQ.iqueue ()

  (* State for join and exit*)
  val numPeers = ref ~1
  val peers = ref (ISS.empty)
  val exitDaemon = ref false

  (* -------------------------------------------------------------------- *)
  (* Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)
  fun debug'' fmsg = debug (fmsg)

  fun contentToStr cnt =
    case cnt of
         S_ACT {aid, value} => concat ["S_ACT[", aidToString aid, "]"]
       | R_ACT {aid} => concat ["R_ACT[", aidToString aid, "]"]
       | S_JOIN {aid} => concat ["S_JOIN[", aidToString aid, "]"]
       | R_JOIN {aid} => concat ["R_JOIN[", aidToString aid, "]"]
       | J_REQ => "J_REQ"
       | J_ACK => "J_ACK"

  fun msgToString (MSG {cid = ChannelId cstr,
                   tid = ThreadId tint,
                   pid = ProcessId nint,
                   cnt}) =
    concat ["MSG -- Channel: ", cstr, " Thread: ", Int.toString tint,
            " Node: ", Int.toString nint, " Request: ", contentToStr cnt]

  val emptyW8Vec = Vector.tabulate (0, fn _ => 0wx0)


  (* -------------------------------------------------------------------- *)
  (* Thread Helper *)
  (* -------------------------------------------------------------------- *)

  fun blockCurrentThread f =
    S.atomicSwitchToNext (fn t =>
      let
        val tid = S.tidInt ()
        val _ = f ()
      in
        blockedThreads := (IntDict.insert (!blockedThreads) tid t)
      end)

  (* -------------------------------------------------------------------- *)
  (* Channel Helper *)
  (* -------------------------------------------------------------------- *)

  fun cleanChannel c {sendQ, recvQ} =
    if IQ.empty sendQ andalso IQ.empty recvQ then
      unmatchedActs := StrDict.remove (!unmatchedActs) c
    else ()

  fun getRecvAct c =
    case StrDict.find (!unmatchedActs) c of
         NONE => NONE
       | SOME {recvQ, sendQ} =>
           if IQ.isEmpty recvQ then NONE
           else
             let
               val result = SOME (IQ.remove recvQ)
               val () = cleanChannel c {sendQ, recvQ}
             in
               result
             end

  fun getSendAct c =
    case StrDict.find (!unmatchedActs) c of
         NONE => NONE
       | SOME {sendQ, recvQ} =>
           if IQ.isEmpty sendQ then NONE
           else
             let
               val result = SOME (IQ.remove sendQ)
               val () = cleanChannel c {sendQ, recvQ}
             in
               result
             end

  fun getQsMaybeCreate c =
    case StrDict.find (!unmatchedActs) c of
         NONE =>
            let
              val v = {sendQ = IQ.iqueue (), recvQ = IQ.iqueue ()}
              val () = unmatchedActs := StrDict.insert (!unmatchedActs) c v
            in
              v
            end
       | SOME v => v

  fun insertSendAct c v =
  let
    val {sendQ, ...} = getQsMaybeCreate c
  in
    IQ.insert sendQ v
  end

  fun insertRecvAct c v =
  let
    val {recvQ, ...} = getQsMaybeCreate c
  in
    IQ.insert recvQ v
  end

  fun insertMatchedAct {waitNode, matchAid} =
    (setMatchedAct waitNode matchAid;
     matchedActs := AISD.insert (!matchedActs) matchAid waitNode)

  fun removeMatchedAct {waitNode} =
  let
    val matchAid = getMatchAid waitNode
    removeMatchedAid waitNode
    matchedActs := AISD.remove (!matchedActs) match

  (* -------------------------------------------------------------------- *)
  (* Message Helper Functions *)
  (* -------------------------------------------------------------------- *)

  (* Send to both local and remote *)
  fun msgSend msg =
  let
    val _ = IQ.insert localMsgQ msg
    val PROXY {sink, ...} = !proxy
    val _ = ZMQ.send (valOf sink, msg)
  in
    ()
  end

  (* Recv from both local and remote *)
  fun msgRecv msg =
    if not (IQ.isEmpty localMsgQ) then
      let
        val result = IQ.front localMsgQ
        val _ = IQ.remove localMsgQ
      in
        SOME (result)
      end
    else
      let
        val PROXY {source, ...} = !proxy
      in
        ZMQ.recvNB (valOf source)
      end


  (* -------------------------------------------------------------------- *)
  (* Server *)
  (* -------------------------------------------------------------------- *)

  fun startProxy {frontend = fe_str, backend = be_str} =
  let
    (* init *)
    val context = ZMQ.ctxNew ()
    val frontend = ZMQ.sockCreate (context, ZMQ.Sub)
    val backend = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockBind (frontend, fe_str)
    val _ = ZMQ.sockBind (backend, be_str)
  in
    ZMQ.proxy {frontend = frontend, backend = backend}
  end

  (* -------------------------------------------------------------------- *)
  (* Client Daemon *)
  (* -------------------------------------------------------------------- *)

  fun processMsg (msg as MSG {cid as ChannelId c, pid as ProcessId n, tid, cnt}) =
  let
    (* Create queue in the pending action hash map if it doesn't exist *)
    fun createQueues () =
    let
      val v = {sendQ = IQ.iqueue (), recvQ = IQ.iqueue ()}
    in
      pendingActions := StrDict.insert (!pendingActions) c v
    end

    (* Remove the channel entry from the pending action hash map if both the
      * queues become empty *)
    fun cleanupQueue (sendq, recvq) =
      if IQ.isEmpty sendq andalso IQ.isEmpty recvq then
        pendingActions := StrDict.remove (!pendingActions) c
      else ()

  in
    case cnt of
        _ => ()
  end

  fun clientDaemon source =
    if (!exitDaemon) then ()
    else
      case ZMQ.recvNB source of
          NONE => (C.yield (); clientDaemon source)
        | SOME m =>
            let
              val _ = debug'' (fn () => msgToString m)
              val _ = processMsg m
            in
              clientDaemon source
            end

  (* -------------------------------------------------------------------- *)
  (* Client *)
  (* -------------------------------------------------------------------- *)

  val yield = C.yield

  fun connect {sink = sink_str, source = source_str, processId = pid, numPeers = np} =
  let
    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
    val _ = ZMQ.sockSetSubscribe (source, Vector.tabulate (0, fn _ => 0wx0))

    val _ = processId := pid
    val _ = numPeers := np

    fun join n =
    let
      val _ = debug' ("DmlDecentralized.connect.join(1)")
      val n = if n=10000 then
                (ZMQ.send (sink, MSG {cid = ChannelId "bogus", pid = ProcessId (!processId),
                          tid = ThreadId ~1, cnt = J_REQ}); 0)
              else n+1
      val () = case ZMQ.recvNB source of
                    NONE => join n
                  | SOME (MSG {pid = ProcessId pidInt, cnt = J_ACK, ...}) =>
                      let
                        val _ = peers := ISS.insert (!peers) pidInt
                      in
                        if ISS.size (!peers) = (!numPeers) then ()
                        else join n
                      end
                  | _ => raise Fail "DmlDecentralized.connect: unexpected message during connect"
    in
      ()
    end
    val _ = join 10000

    (* If we get here, then it means that we have joined *)
    val _ = proxy := PROXY {context = SOME context,
                            source = SOME source,
                            sink = SOME sink}
  in
    ()
  end

  fun runDML (f, to) =
    let
      val _ = Assert.assert ([], fn () => "runDML must be run after connect",
                             fn () => case !proxy of
                                        PROXY {sink = NONE, ...} => false
                                      | _ => true)
      fun body () =
      let
        val PROXY {source, ...} = !proxy
        (* start the daemon *)
        val _ = C.spawn (fn () => clientDaemon (valOf source))
      in
        f ()
      end
      val _ = RunCML.doit (body, to)
      val PROXY {source, sink, ...} = !proxy
      val _ = ZMQ.sockClose (valOf source)
      val _ = ZMQ.sockClose (valOf sink)
    in
      OS.Process.success
    end

  fun channel s = CHANNEL (ChannelId s)


  fun send (CHANNEL c, m) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlDecentralized.send(1)")
    val {actAid, waitNode} = handleSend {cid = c}
    val m = MLton.serialize (m)
  in
    case getRecvAct c of
         NONE => blockCurrentThread (fn () =>
           let
             val _ = debug' ("DmlDecentralized.send(2.1)")
             val _ = msgSend (MSG {cid = c, aid = actAid, cnt = S_ACT m})
             val _ = debug' ("DmlDecentralized.send(2.2)")
           in
             ()
           end) (* Implicit atomic end *)
       | SOME {aid = recvAid} =>
           blockCurrentThread (fn () =>
            let
              val _ = debug' ("DmlDecentralized.send(3.1)")
              val _ = setMatchAct waitNode recvAid
              val _ = msgSend (MSG {cid = c, aid = actAid, cnt = S_JOIN recvAid})
              val _ = debug' ("DmlDecentralized.send(3.2)")
            in
              ()
            end



  fun recv (CHANNEL c) =
  let
    val _ = debug' ("DmlDecentralized.recv(1)")
    val {actAid, ...} = handleRecv {cid = c}
    val serM =
      S.switchToNext (fn t : w8vec S.thread =>
        let
          val tid = S.tidInt ()
          val _ = debug' ("DmlDecentralized.send(2)")
          val _ = debug' ("DmlDecentralized.send(3)")
        in
          ()
        end)
  in
    MLton.deserialize serM
  end

  val exitDaemon = fn () => exitDaemon := true

  fun spawn f = ignore (C.spawn f)

end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
