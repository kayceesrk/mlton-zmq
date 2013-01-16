(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure DmlCentralizedDepGraph : DML =
struct
  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)

  structure ZMQ = MLton.ZMQ
  structure RI = IntRedBlackDict
  structure RS = StringRedBlackDict
  structure S = CML.Scheduler
  structure C = CML
  structure IQ = IQueue

  open RepTypes
  open StableGraph


  (* -------------------------------------------------------------------- *)
  (* Datatype definitions *)
  (* -------------------------------------------------------------------- *)


  datatype content = S_REQ of w8vec
                   | R_REQ
                   | S_ACK of {matchAid: action_id} (* Acknowledgement inclues the matching action id *)
                   | R_ACK of {matchAid: action_id, value: w8vec} (* Acknowledgement includes the matching action id *)
                   | J_REQ
                   | J_ACK


  datatype msg = MSG of {cid : channel_id,
                         aid : action_id,
                         cnt : content}

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val blockedThreads = ref (RI.empty)
  val pendingActions = ref (RS.empty)
  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})
  val exitDaemon = ref false

  (* -------------------------------------------------------------------- *)
  (* Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)
  fun debug'' fmsg = debug (fmsg)

  fun contentToStr cnt =
    case cnt of
         S_REQ _  => "S_REQ"
       | R_REQ    => "R_REQ"
       | S_ACK _  => "S_ACK"
       | R_ACK _  => "R_ACK"
       | J_REQ    => "J_REQ"
       | J_ACK    => "J_ACK"

  fun msgToString (MSG {cid = ChannelId cstr, aid, cnt}) =
    concat ["MSG -- ", aidToString aid, " Channel: ", cstr," Request: ", contentToStr cnt]

  val emptyW8Vec = Vector.tabulate (0, fn _ => 0wx0)


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
    val _ = ZMQ.sockSetSubscribe (frontend, Vector.tabulate (0, fn _ => 0wx0))

    fun processMsg (msg as MSG {cid as ChannelId c, aid, cnt}) =
    let
      (* Create queue in the pending action hash map if it doesn't exist *)
      fun createQueues () =
      let
        val v = {sendQ = IQ.iqueue (), recvQ = IQ.iqueue ()}
      in
        pendingActions := RS.insert (!pendingActions) c v
      end

      (* Remove the channel entry from the pending action hash map if both the
       * queues become empty *)
      fun cleanupQueue (sendq, recvq) =
        if IQ.isEmpty sendq andalso IQ.isEmpty recvq then
          pendingActions := RS.remove (!pendingActions) c
        else ()

    in
      case cnt of
           J_REQ (* Node wants to join *) => (* reply with J_ACK *)
             let
               val prefix = MLton.serialize (aidToPidInt aid)
             in
               ZMQ.sendWithPrefix (backend, MSG {cid = cid, aid = aid, cnt = J_ACK}, prefix)
             end
         | S_REQ data =>
             (case RS.find (!pendingActions) c of
                   NONE => (createQueues (); processMsg msg)
                 | SOME {sendQ,recvQ} =>
                     if IQ.isEmpty recvQ then (* No matching receives *)
                       IQ.insert sendQ msg
                     else
                       let
                         val MSG m' = IQ.front recvQ
                         val _ = IQ.remove recvQ

                         (* recv acknowledgement *)
                         val recvAck = MSG {cid = #cid m', aid = #aid m', cnt = R_ACK {matchAid = aid, value = data}}
                         val prefix = MLton.serialize (aidToPidInt (#aid m'))
                         val _ = ZMQ.sendWithPrefix (backend, recvAck, prefix)

                         (* send acknowledgement *)
                         val sendAck = MSG {cid = cid, aid = aid, cnt = S_ACK {matchAid = #aid m'}}
                         val prefix = MLton.serialize (aidToPidInt (aid))
                         val _ = ZMQ.sendWithPrefix (backend, sendAck, prefix)
                       in
                         cleanupQueue (sendQ, recvQ)
                       end)
         | R_REQ =>
             (case RS.find (!pendingActions) c of
                   NONE => (createQueues (); processMsg msg)
                 | SOME {sendQ, recvQ} =>
                     if IQ.isEmpty sendQ then
                       IQ.insert recvQ msg
                     else
                       let
                         val MSG m' = IQ.front sendQ
                         val _ = IQ.remove sendQ

                         (* send acknowledgement *)
                         val sendAck = MSG {cid = #cid m', aid = #aid m', cnt = S_ACK {matchAid = aid}}
                         val prefix = MLton.serialize (aidToPidInt (#aid m'))
                         val _ = ZMQ.sendWithPrefix (backend, sendAck, prefix)

                         (* recv acknowledgement *)
                         val data = case #cnt m' of
                             S_REQ data => data
                           | _ => raise Fail "DmlCentralized.processMessage.R_REQ.SOME: unexpected"
                         val recvAck = MSG {cid = cid, aid = aid, cnt = R_ACK {matchAid = #aid m', value = data}}
                         val prefix = MLton.serialize (aidToPidInt aid)
                         val _ = ZMQ.sendWithPrefix (backend, recvAck, prefix)
                       in
                         cleanupQueue (sendQ, recvQ)
                       end)
         | _ => ()
    end


    (* main loop *)
    fun processLoop () =
    let
      val _ = debug' ("DmlCentralized.startProxy.processLoop(1)")
      val m : msg = ZMQ.recv frontend
      val _ = debug'' (fn () => (msgToString m))
      val _ = processMsg m
    in
      processLoop ()
    end

    val _ = debug' "DmlCentralized.startProxy: starting processLoop"
    val _ = processLoop ()
  in
    ()
  end

  (* -------------------------------------------------------------------- *)
  (* Clients *)
  (* -------------------------------------------------------------------- *)

  fun clientDaemon source =
    if (!exitDaemon) then ()
    else
      case ZMQ.recvNB source of
          NONE => (C.yield (); clientDaemon source)
        | SOME (m as MSG {aid, cnt, ...}) =>
            let
              val tidInt = aidToTidInt aid
              val _ = debug'' (fn () => msgToString m)
              val t = RI.lookup (!blockedThreads) tidInt
              val _ = blockedThreads := RI.remove (!blockedThreads) tidInt
              fun setMatch matchAid =
              let
                val tid = S.getThreadId t
                val node = valOf (!(C.tidToNode tid))
              in
                setMatchAct (node, matchAid)
              end
              val _ =
                  (case cnt of
                      S_ACK {matchAid} =>
                        let
                          val _ = setMatch matchAid
                        in
                          S.doAtomic (fn () => S.ready (S.prepVal (t, emptyW8Vec)))
                        end
                    | R_ACK {matchAid, value = m} =>
                        let
                          val _ = setMatch matchAid
                        in
                          S.doAtomic (fn () => S.ready (S.prepVal (t, m)))
                        end
                    | _ => ())
            in
              clientDaemon source
            end

  val yield = C.yield

  fun connect {sink = sink_str, source = source_str, processId = pid} =
  let
    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
    val _ = processId := pid

    (* Set filter to receive only those messages addresed to me *)
    val filter = MLton.serialize pid
    val _ = ZMQ.sockSetSubscribe (source, filter)

    fun join n =
    let
      val _ = debug' ("DmlCentralized.connect.join(1)")
      val n = if n=1000 then
                (ZMQ.send (sink, MSG {cid = ChannelId "bogus", cnt = J_REQ,
                                      aid = ACTION_ID {pid = ProcessId (!processId), tid = ThreadId ~1,
                                                       aid = ~1, rid = ~1}});
                 0)
              else n+1
      val m : msg = case ZMQ.recvNB source of
                            NONE => join n
                          | SOME m => m
    in
      m
    end
    val _ = join 1000

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
        val _ = handleInit ()
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
    val _ = debug' ("DmlCentralized.send(1)")
    val aid = handleSend {cid = c}
    val m = MLton.serialize (m)
    val _ = S.switchToNext (fn t : w8vec S.thread =>
              let
                val tid = S.tidInt ()
                val PROXY {sink, ...} = !proxy
                val _ = debug' ("DmlCentralized.send(2)")
                val _ = ZMQ.send (valOf sink, MSG {cid = c, aid = aid, cnt = S_REQ m})
                val _ = blockedThreads := (RI.insert (!blockedThreads) tid t)
                val _ = debug' ("DmlCentralized.send(3)")
              in
                ()
              end)
  in
    ()
  end

  fun recv (CHANNEL c) =
  let
    val aid = handleRecv {cid = c}
    val serM =
      S.switchToNext (fn t : w8vec S.thread =>
        let
          val tid = S.tidInt ()
          val PROXY {sink, ...} = !proxy
          val _ = ZMQ.send (valOf sink, {cid = c, aid = aid, cnt = R_REQ})
          val _ = blockedThreads := (RI.insert (!blockedThreads) tid t)
        in
          ()
        end)
  in
    MLton.deserialize serM
  end

  val exitDaemon = fn () => exitDaemon := true

  fun spawn f =
    let
      val tid = S.newTid ()
      val _ = handleSpawn {newTid = tid}
    in
      ignore (C.spawnWithTid (f, tid))
    end

end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
