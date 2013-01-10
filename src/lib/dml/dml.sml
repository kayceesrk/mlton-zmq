(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure DmlCentralized : DML =
struct
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  structure ZMQ = MLton.ZMQ
  structure RI = IntRedBlackDict
  structure RS = StringRedBlackDict
  structure S = CML.Scheduler
  structure C = CML
  structure IQ = IQueue


  (* -------------------------------------------------------------------- *)
  (* Datatype definitions *)
  (* -------------------------------------------------------------------- *)

  type w8vec = Word8.word vector

  datatype thread_id  = ThreadId of int
  datatype node_id    = NodeId of int
  datatype channel_id = ChannelId of string


  datatype proxy = PROXY of {context : ZMQ.context option,
                             sink: ZMQ.socket option,
                             source: ZMQ.socket option}

  datatype 'a chan = CHANNEL of channel_id

  datatype content = S_REQ of w8vec
                   | R_REQ
                   | S_ACK
                   | R_ACK of w8vec
                   | J_REQ
                   | J_ACK


  datatype msg = MSG of {cid : channel_id,
                         nid : node_id,
                         tid : thread_id,
                         cnt : content}

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val blockedThreads = ref (RI.empty)
  val pendingActions = ref (RS.empty)
  val nodeId = ref ~1
  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})

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
       | S_ACK    => "S_ACK"
       | R_ACK _  => "R_ACK"
       | J_REQ    => "J_REQ"
       | J_ACK    => "J_ACK"

  fun msgToString (MSG {cid = ChannelId cstr,
                   tid = ThreadId tint,
                   nid = NodeId nint,
                   cnt}) =
    concat ["MSG -- Channel: ", cstr, " Thread: ", Int.toString tint,
            " Node: ", Int.toString nint, " Request: ", contentToStr cnt]

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

    fun processMsg (msg as MSG {cid as ChannelId c, nid as NodeId n, tid, cnt}) =
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
           J_REQ (* Node want to join *) => (* reply with J_ACK *)
             let
               val prefix = MLton.serialize nid
             in
               ZMQ.sendWithPrefix (backend, MSG {cid = cid, nid = nid, tid = tid, cnt = J_ACK}, prefix)
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
                         val recvAck = MSG {cid = #cid m', tid = #tid m', nid = #nid m', cnt = R_ACK data}
                         val prefix = MLton.serialize (#nid m')
                         val _ = ZMQ.sendWithPrefix (backend, recvAck, prefix)

                         (* send acknowledgement *)
                         val sendAck = MSG {cid = cid, nid = nid, tid = tid, cnt = S_ACK}
                         val prefix = MLton.serialize n
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
                         val sendAck = MSG {cid = #cid m', tid = #tid m', nid = #nid m', cnt = S_ACK}
                         val prefix = MLton.serialize (#nid m')
                         val _ = ZMQ.sendWithPrefix (backend, sendAck, prefix)

                         (* recv acknowledgement *)
                         val data = case #cnt m' of
                             S_REQ data => data
                           | _ => raise Fail "DmlCentralized.processMessage.R_REQ.SOME: unexpected"
                         val recvAck = MSG {cid = cid, nid = nid, tid = tid, cnt = R_ACK data}
                         val prefix = MLton.serialize n
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
    case ZMQ.recvNB source of
         NONE => (C.yield (); clientDaemon source)
       | SOME (m as MSG {tid = ThreadId tidInt, cnt, ...}) =>
           let
             val _ = debug'' (fn () => msgToString m)
             val t = RI.lookup (!blockedThreads) tidInt
             val _ = blockedThreads := RI.remove (!blockedThreads) tidInt
             val _ =
                (case cnt of
                     S_ACK => S.doAtomic (fn () => S.ready (S.prepVal (t, emptyW8Vec)))
                   | R_ACK m => S.doAtomic (fn () => S.ready (S.prepVal (t, m)))
                   | _ => ())
           in
             clientDaemon source
           end

  fun connect {sink = sink_str, source = source_str, nodeId = nid} =
  let
    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
    val _ = nodeId := nid

    (* Set filter to receive only those messages addresed to me *)
    val filter = MLton.serialize nid
    val _ = ZMQ.sockSetSubscribe (source, filter)

    fun join n =
    let
      val _ = debug' ("DmlCentralized.connect.join(1)")
      val n = if n=1000 then
                (ZMQ.send (sink, MSG {cid = ChannelId "bogus", nid = NodeId (!nodeId),
                          tid = ThreadId ~1, cnt = J_REQ}); 0)
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
        val PROXY {source, ...} = !proxy
        (* start the daemon *)
        val _ = C.spawn (fn () => clientDaemon (valOf source))
      in
        f ()
      end
    in
      RunCML.doit (body, to)
    end

  fun channel s = CHANNEL (ChannelId s)

  fun send (CHANNEL c, m) =
  let
    val _ = debug' ("DmlCentralized.send(1)")
    val m = MLton.serialize (m)
    val _ = S.switchToNext (fn t : w8vec S.thread =>
              let
                val tid = S.tidInt ()
                val PROXY {sink, ...} = !proxy
                val _ = debug' ("DmlCentralized.send(2)")
                val _ = ZMQ.send (valOf sink, MSG {cid = c, nid = NodeId (!nodeId),
                                  tid = ThreadId tid, cnt = S_REQ m})
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
    val serM =
      S.switchToNext (fn t : w8vec S.thread =>
        let
          val tid = S.tidInt ()
          val PROXY {sink, ...} = !proxy
          val _ = ZMQ.send (valOf sink, {cid = c, nid = NodeId (!nodeId),
                            tid = ThreadId tid, cnt = R_REQ})
          val _ = blockedThreads := (RI.insert (!blockedThreads) tid t)
        in
          ()
        end)
  in
    MLton.deserialize serM
  end
  (* -------------------------------------------------------------------- *)
end

structure Dml = DmlCentralized

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
