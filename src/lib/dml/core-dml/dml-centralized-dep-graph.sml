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
  structure RI  = IntRedBlackDict
  structure RS  = StringRedBlackDict
  structure ISD = IntSplayDict
  structure S   = CML.Scheduler
  structure C   = CML
  structure IQ  = IQueue
  structure CR  = Critical

  open RepTypes
  open ActionManager


  (* -------------------------------------------------------------------- *)
  (* Datatype definitions *)
  (* -------------------------------------------------------------------- *)


  datatype content = S_REQ of w8vec
                   | R_REQ
                   | S_ACK of {matchAid: action_id} (* Acknowledgement inclues the matching action id *)
                   | R_ACK of {matchAid: action_id, value: w8vec} (* Acknowledgement includes the matching action id *)
                   | J_REQ
                   | J_ACK
                   | RB_REQ of {visitedSet: AISS.set, aidList: action_id list}


  datatype msg = MSG of {cid : channel_id,
                         aid : action_id,
                         cnt : content}


  structure PTROrdered :> ORDERED
    where type t = action_id =
  struct
    type t = action_id
    fun eq (i1, i2) = MLton.equal (i1, i2)
    fun compare (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, ...},
                  ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, ...}) =
      (case Int.compare (pid1, pid2) of
            EQUAL => (case Int.compare (tid1, tid2) of
                          EQUAL => Int.compare (rid1, rid2)
                        | lg => lg)
          | lg => lg)
  end
  structure PTRSet = SplaySet (structure Elem = PTROrdered)

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})

  (* Client Only *)
  val blockedThreads = ref (RI.empty)
  val allThreads = ref (RI.empty)
  val exitDaemon = ref false

  (* Broker Only *)
  val pendingActions = ref (RS.empty)

  (* -------------------------------------------------------------------- *)
  (* Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  fun contentToStr cnt =
    case cnt of
         S_REQ _  => "S_REQ"
       | R_REQ    => "R_REQ"
       | S_ACK {matchAid, ...}  => concat ["S_ACK[", aidToString matchAid,"]"]
       | R_ACK {matchAid, ...}  => concat ["R_ACK[", aidToString matchAid,"]"]
       | J_REQ    => "J_REQ"
       | J_ACK    => "J_ACK"
       | RB_REQ {visitedSet, aidList} => concat ["RB_REQ(", Int.toString (AISS.size visitedSet), ",", Int.toString (length aidList), ")"]

  fun msgToString (MSG {cid = ChannelId cstr, aid, cnt}) =
    concat ["Aid: ", aidToString aid, " Channel: ", cstr," Request: ", contentToStr cnt]

  val emptyW8Vec = Vector.tabulate (0, fn _ => 0wx0)

  fun addToAllThreads () =
  let
    val newAllThreads = RI.insert (!allThreads) (S.tidInt ()) (S.getCurThreadId ())
  in
    allThreads := newAllThreads
  end

  fun removeFromAllThreads () =
  let
    val newAllThreads = RI.remove (!allThreads) (S.tidInt ())
  in
    allThreads := newAllThreads
  end

  fun tid2tid (ThreadId tid) = RI.find (!allThreads) tid

  fun partitionRollbackList l =
    ListMLton.fold (l, ISD.empty,
      fn (aid as ACTION_ID {pid = ProcessId pidInt, ...}, dict) =>
        case ISD.find dict pidInt of
             NONE => ISD.insert dict pidInt [aid]
           | SOME l' => ISD.insert dict pidInt (aid::l'))

  fun performLocalRollbackRemoteMsgs {localRestore, remoteRollbacks, visitedSet} =
  let
    val _ = Assert.assertAtomic' ("performLocalRollbackRemoteMsgs", NONE)
    val PROXY {sink, ...} = !proxy

    (* Process remote rollbacks first *)
    val remoteRBList = ISD.toList (partitionRollbackList remoteRollbacks)
    val _ = ListMLton.map (remoteRBList, fn (pidInt, aidList) =>
              let
                val prefix = MLton.serialize pidInt
                val _ = debug (fn () => "Rollback: sending rollback message to process " ^(Int.toString pidInt))
                val _ = debug (fn () => ListMLton.fold (aidList, "", fn (aid, str) => (aidToString aid)^" "^str))
              in
                ZMQ.sendWithPrefix (valOf sink,
                                    MSG {cid = ChannelId "bogus", aid = dummyAid (),
                                         cnt = RB_REQ {visitedSet = visitedSet, aidList = aidList}},
                                    prefix)
              end)

    (* Convert localRestores from AISS.set to PTRSet.set *)
    (* Each thread has exactly one saved continuation, associated with a
    * corresponding revision number. This step is needed because we might have
    * the target thread already rollback beyond the point to which we want the
    * threads to rollback. Such threads will have a revision id which is
    * greater than the revision id of the thread we are looking to rollback to.
    * Hence, we convert action_id to {pid, tid, rid}, look for a matching
    * candidate. If the target thread is still in the revision we want, then we
    * rollback this thread, otherwise, we leave it untouched. *)
    val localRestore : PTRSet.set = AISS.foldl (fn (e, acc) => PTRSet.insert acc e) PTRSet.empty localRestore

    (* Process local restores on Scheduler *)
    fun restoreSCore (S.RTHRD (tid, t)) =
      if not (PTRSet.member localRestore (getAidFromTid tid)) then
        S.RTHRD (tid, t)
      else
        S.RTHRD (tid, MLton.Thread.prepare (MLton.Thread.new (restoreCont), ()))
    val () = S.modify restoreSCore

    (* process local restores on blocked threads *)
    fun restoreBTCore (k, t as S.THRD (tid,_),acc) =
     if not (PTRSet.member localRestore (getAidFromTid tid)) then (RI.insert acc k t)
      else
        let
          (* XXX KC: why not remove the thread from the scheduler instead of raising Kill? *)
          val prolog = fn () => (restoreCont (); emptyW8Vec)
          val rt = S.prep (S.prepend (t, prolog))
          val _ = S.ready rt
        in
          acc
        end
    val newBT = RI.foldl restoreBTCore RI.empty (!blockedThreads)
    val _ = blockedThreads := newBT
  in
    ()
  end

  fun rollback () =
  let
    val _ = CR.atomicBegin ()
    val startNode = valOf (!(S.tidNode ()))

    (* dfs *)
    val {localRestore, remoteRollbacks, visitedSet} =
      rhNodeToThreads {startNode = startNode, tid2tid = tid2tid, visitedSet = AISS.empty}

    val _ = performLocalRollbackRemoteMsgs
              {localRestore = localRestore,
               remoteRollbacks = remoteRollbacks,
               visitedSet = visitedSet}
    val _ = CR.atomicEnd ()

    (* Finally rollback *)
    val _ = restoreCont ()
  in
    ()
  end

  fun processRollbackMsg {visitedSet, aidList} =
  let
    val _ = CR.atomicBegin ()
    val _ = debug' ("processRollbackMsg(1)")
    val nodeList = ListMLton.fold (aidList, [],
                    fn (aid, acc) => case aidToNode (aid, tid2tid) of
                                          NONE => acc
                                        | SOME n => n::acc)
    val _ = debug' ("processRollbackMsg(2)")
    val (lr, rrb, vs) = ListMLton.fold (nodeList, (AISS.empty, [], visitedSet),
        fn (startNode, (lr, rrb, vs)) =>
        let
          val {localRestore, remoteRollbacks, visitedSet} =
            rhNodeToThreads {startNode = startNode, tid2tid = tid2tid, visitedSet = vs}
        in
          (AISS.union localRestore lr, rrb @ remoteRollbacks, visitedSet)
        end)
    val _ = debug' ("processRollbackMsg(3)")
    val _ = performLocalRollbackRemoteMsgs
              {localRestore = lr, remoteRollbacks = rrb, visitedSet = vs}
    val _ = debug' ("processRollbackMsg(4)")
  in
    CR.atomicEnd ()
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
         | RB_REQ {visitedSet, aidList} =>
             (* Forward the request to the intended recipient *)
             let
               val recipientPidInt = aidToPidInt (hd aidList) (* aidList is always non-empty *)
               val prefix = MLton.serialize recipientPidInt
             in
               ZMQ.sendWithPrefix (backend, msg, prefix)
             end
         | _ => ()
    end


    (* main loop *)
    fun processLoop () =
    let
      val _ = debug' ("DmlCentralized.startProxy.processLoop(1)")
      val m : msg = ZMQ.recv frontend
      val _ = debug (fn () => (msgToString m))
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
          NONE =>
            let
              val _ = C.yield ()
            in
              clientDaemon source
            end
        | SOME (m as MSG {aid, cnt, ...}) =>
            let
              val tidInt = aidToTidInt aid
              val _ = debug (fn () => "DAEMON: " ^ (msgToString m))
              val _ =
                  (case cnt of
                      S_ACK {matchAid} =>
                        let
                          val t = RI.lookup (!blockedThreads) tidInt
                          val waitNode = valOf (!(C.tidToNode (S.getThreadId t)))
                          val _ = blockedThreads := RI.remove (!blockedThreads) tidInt
                          val _ = setMatchAct waitNode matchAid
                        in
                          S.doAtomic (fn () => S.ready (S.prepVal (t, emptyW8Vec)))
                        end
                    | R_ACK {matchAid, value = m} =>
                        let
                          val t = RI.lookup (!blockedThreads) tidInt
                          val waitNode = valOf (!(C.tidToNode (S.getThreadId t)))
                          val _ = blockedThreads := RI.remove (!blockedThreads) tidInt
                          val _ = setMatchAct waitNode matchAid
                        in
                          S.doAtomic (fn () => S.ready (S.prepVal (t, m)))
                        end
                    | RB_REQ {visitedSet, aidList} =>
                       processRollbackMsg {visitedSet = visitedSet, aidList = aidList}
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
                                      aid = dummyAid ()});
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
        val PROXY {source, ...} = !proxy
        (* start the daemon. Insert a CR_RB node just so that functions which
         * expect a node at every tid will not throw exceptions. *)
        val _ = C.spawn (fn () =>
                  let
                    val _ = insertCommitNode ()
                  in
                    clientDaemon (valOf source)
                  end)
        val _ = addToAllThreads ()
        val _ = insertCommitNode ()
        val _ = saveCont ()
        fun safeBody () = (removeFromAllThreads (f ())) handle e => (removeFromAllThreads ();
                                                               case e of
                                                                    CML.Kill => ()
                                                                  | _ => raise e)
        val _ = safeBody ()
      in
        ()
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
    val {actAid, ...} = handleSend {cid = c}
    val m = MLton.serialize (m)
    val _ =
      S.switchToNext (fn t : w8vec S.thread =>
        let
          val tid = S.tidInt ()
          val PROXY {sink, ...} = !proxy
          val _ = debug' ("DmlCentralized.send(2)")
          val _ = ZMQ.send (valOf sink, MSG {cid = c, aid = actAid, cnt = S_REQ m})
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
    val _ = debug' ("DmlCentralized.recv(1)")
    val {actAid, ...} = handleRecv {cid = c}
    val serM =
      S.switchToNext (fn t : w8vec S.thread =>
        let
          val tid = S.tidInt ()
          val PROXY {sink, ...} = !proxy
          val _ = debug' ("DmlCentralized.recv(2)")
          val _ = ZMQ.send (valOf sink, {cid = c, aid = actAid, cnt = R_REQ})
          val _ = blockedThreads := (RI.insert (!blockedThreads) tid t)
          val _ = debug' ("DmlCentralized.recv(3)")
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
      val tidInt = C.tidToInt tid
      val aid = handleSpawn {childTid = ThreadId tidInt}
      fun prolog () =
        let
          val _ = addToAllThreads ()
        in
          handleInit {parentAid = aid}
        end
      fun safeBody () = (removeFromAllThreads(f(prolog()))) handle e => (removeFromAllThreads ();
                                                                     case e of
                                                                          CML.Kill => ()
                                                                        | _ => raise e)
    in
      ignore (C.spawnWithTid (safeBody, tid))
    end



end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
