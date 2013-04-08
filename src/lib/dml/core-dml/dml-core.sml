(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)


signature IVAR =
sig
  val new : unit -> {read: unit -> 'a, write: 'a -> unit}
end

structure DmlCore : DML_INTERNAL =
struct
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  open RepTypes
  open ActionHelper
  open CommunicationHelper
  open GraphManager
  open Arbitrator

  structure IntDict = IntSplayDict
  structure S = CML.Scheduler
  structure C = CML
  structure ISS = IntSplaySet
  structure ZMQ = MLton.ZMQ

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (* -------------------------------------------------------------------- *)
  (* Datatype and value definitions *)
  (* -------------------------------------------------------------------- *)

  datatype 'a chan = CHANNEL of channel_id
  val emptyW8Vec : w8vec = Vector.tabulate (0, fn _ => 0wx0)

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val pendingLocalSends : {sendWaitNode : node, value : w8vec} PendingComm.t = PendingComm.empty ()
  val pendingLocalRecvs : {recvWaitNode : node} PendingComm.t = PendingComm.empty ()
  val pendingRemoteSends : w8vec PendingComm.t = PendingComm.empty ()
  val pendingRemoteRecvs : unit PendingComm.t = PendingComm.empty ()

  val matchedSends : w8vec MatchedComm.t = MatchedComm.empty ()
  val matchedRecvs : w8vec MatchedComm.t = MatchedComm.empty ()

  val blockedThreads : w8vec S.thread IntDict.dict ref = ref (IntDict.empty)

  (* State for join and exit*)
  val peers = ref (ISS.empty)
  val exitDaemon = ref false

  (* Commit function *)
  val commitRef = ref (fn () => ())

  (* -------------------------------------------------------------------- *)
  (* Scheduler Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun blockCurrentThread () =
  let
    val _ = Assert.assertAtomic' ("DmlDecentalized.blockCurrentThread", SOME 1)
    val tidInt = S.tidInt ()
  in
    S.atomicSwitchToNext (fn t => blockedThreads := IntDict.insert (!blockedThreads) tidInt t)
  end

  fun resumeThread tidInt (value : w8vec) =
  let
    val _ = Assert.assertAtomic' ("DmlCore.unblockthread", NONE)
    val t = IntDict.lookup (!blockedThreads) tidInt
    val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
    val rt = S.prepVal (t, value)
  in
    S.ready rt
  end handle IntDict.Absent => ()

  fun resumeThreadIfLastAidIs aid tidInt (value : w8vec) =
  let
    val _ = Assert.assertAtomic' ("DmlCore.unblockthread", SOME 1)
    val t = IntDict.lookup (!blockedThreads) tidInt
    val _ = if not (isLastAidOnThread (t, aid)) then raise IntDict.Absent else ()
    val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
    val rt = S.prepVal (t, value)
  in
    S.ready rt
  end handle IntDict.Absent => ()

  fun rollbackBlockedThreads ptrDict =
  let
    val _ = Assert.assertAtomic' ("DmlCore.rollbackBlockedThreads", SOME 1)
    fun rollbackBlockedThread t actNum =
      let
        val prolog = fn () => (GraphManager.restoreCont actNum; emptyW8Vec)
        val rt = S.prep (S.prepend (t, prolog))
        val _ = S.ready rt
      in
        ()
      end
    val newBTDict =
      IntDict.foldl (fn (tidInt, t as S.THRD (tid, _), newBTDict) =>
        let
          val pid = ProcessId (!processId)
          val rid = CML.tidToRev tid
          val tid = ThreadId tidInt
        in
          case PTRDict.find ptrDict {pid = pid, tid = tid, rid = rid} of
                NONE => IntDict.insert newBTDict tidInt t
              | SOME actNum => (rollbackBlockedThread t actNum; newBTDict)
        end) IntDict.empty (!blockedThreads)
  in
    blockedThreads := newBTDict
  end

  fun rollbackReadyThreads ptrDict =
  let
    val _ = Assert.assertAtomic' ("DmlCore.rollbackReadyThreads", SOME 1)
    fun restoreSCore (rthrd as S.RTHRD (cmlTid, _)) =
      let
        val pid = ProcessId (!processId)
        val rid = CML.tidToRev cmlTid
        val tid = ThreadId (CML.tidToInt cmlTid)
      in
        case PTRDict.find ptrDict {pid = pid, tid = tid, rid = rid} of
             NONE => rthrd
           | SOME actNum => S.RTHRD (cmlTid, MLton.Thread.prepare
              (MLton.Thread.new (fn () => restoreCont actNum), ()))
      end
  in
    S.modify restoreSCore
  end

  (* -------------------------------------------------------------------- *)
  (* Simple IVar *)
  (* -------------------------------------------------------------------- *)


  structure IVar : IVAR =
  struct
    structure Assert = LocalAssert(val assert = true)
    structure Debug = LocalDebug(val debug = false)

    fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
    (* fun debug' msg = debug (fn () => msg) *)

    datatype 'a k = THREAD of thread_id
                  | VALUE of 'a
                  | EMPTY

    fun kToString (k) =
      case k of
           THREAD (ThreadId tidInt) => concat ["Thread(", Int.toString tidInt, ")"]
         | EMPTY => "EMPTY"
         | VALUE _ => "VALUE"

    fun new () =
    let
      val r = ref EMPTY
      fun write v =
      let
        val _ = debug' ("IVar.write(1)")
        val _ = S.atomicBegin ()
        val _ = debug (fn () => ("IVar.write: "^(kToString (!r))))
        val _ = case (!r) of
                     EMPTY => r := VALUE v
                   | THREAD (ThreadId tidInt) => (r := VALUE v; resumeThread tidInt emptyW8Vec)
                   | VALUE _ => raise Fail "IVar.read: Filled with value!"
        val _ = S.atomicEnd ()
      in
        ()
      end
      fun read () =
      let
        val _ = Assert.assertNonAtomic' ("IVar.read(1)")
        val _ = debug' ("IVar.read(1)")
        val _ = S.atomicBegin ()
        val _ = debug (fn () => ("IVar.read: "^(kToString (!r))))
        val v = case (!r) of
                     EMPTY =>
                     let
                       val tidInt = S.tidInt ()
                       val _ = r := THREAD (ThreadId tidInt)
                       val _ = blockCurrentThread ()
                     in
                       read ()
                     end
                   | THREAD _ =>
                     let (* KC: If the blocked thread was rolledback, this branch is possible *)
                       val tidInt = S.tidInt ()
                       val _ = r := THREAD (ThreadId tidInt)
                       val _ = blockCurrentThread ()
                     in
                       read ()
                     end
                   | VALUE v => (S.atomicEnd (); v)
        val _ = Assert.assertNonAtomic' ("IVar.read(2)")
      in
        v
      end
    in
      {read = read, write = write}
    end
  end

  (* -------------------------------------------------------------------- *)
  (* Proxy Server *)
  (* -------------------------------------------------------------------- *)

  fun startProxy {frontend = fe_str, backend = be_str} =
  let
    (* init *)
    val context = ZMQ.ctxNew ()
    val frontend = ZMQ.sockCreate (context, ZMQ.XSub)
    val backend = ZMQ.sockCreate (context, ZMQ.XPub)
    val _ = ZMQ.sockBind (frontend, fe_str)
    val _ = ZMQ.sockBind (backend, be_str)
  in
    ZMQ.proxy {frontend = frontend, backend = backend}
  end

  (* -------------------------------------------------------------------- *)
  (* Client Daemon *)
  (* -------------------------------------------------------------------- *)

  datatype caller_kind = Client | Daemon

  fun processSend {channel = c, sendActAid, sendWaitNode, value} =
  let
    val _ = Assert.assertAtomic' ("DmlCore.processSend(1)", SOME 1)
    val _ = debug' ("DmlCore.processSend(1)")
    val sendActAid = aidIncVersion sendActAid
    val _ =
      case PendingComm.deque pendingLocalRecvs c {againstAid = sendActAid} of
          NONE => (* No matching receives, check remote *)
            let
              val _ = Assert.assertAtomic' ("DmlCore.processSend(2)", SOME 1)
              val _ = debug' ("DmlCore.processSend(2)")
            in
              case PendingComm.deque pendingRemoteRecvs c {againstAid = sendActAid} of
                    NONE => (* No matching remote recv either *)
                      let
                        val _ = Assert.assertAtomic' ("DmlCore.processSend(3)", SOME 1)
                        val _ = debug' ("DmlCore.processSend(3)")
                        val _ = msgSend (S_ACT {channel = c, sendActAid = sendActAid, value = value})
                        val _ = PendingComm.addAid pendingLocalSends c sendActAid
                          {sendWaitNode = sendWaitNode, value = value}
                      in
                        ()
                      end
                  | SOME (recvActAid, ()) => (* matching remote recv *)
                      let
                        val _ = Assert.assertAtomic' ("DmlCore.processSend(4)", SOME 1)
                        val _ = debug' ("DmlCore.processSend(4)")
                        val _ = msgSend (S_ACT {channel = c, sendActAid = sendActAid, value = value})
                        val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                        val _ = MatchedComm.add matchedSends {channel = c, actAid = sendActAid,
                                  remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
                      in
                        ()
                      end
            end
        | SOME (recvActAid, {recvWaitNode}) => (* matching local recv *)
            let
              val _ = Assert.assertAtomic' ("DmlCore.processSend(5)", SOME 1)
              val _ = debug' ("DmlCore.processSend(5)")
              val _ = setMatchAid sendWaitNode recvActAid value
              val _ = setMatchAid recvWaitNode sendActAid emptyW8Vec
              val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              (* Resume blocked recv *)
              val _ = resumeThreadIfLastAidIs (getNextAid recvActAid) (aidToTidInt recvActAid) value
            in
              ()
            end
    val _ = debug' ("DmlCore.processSend(6)")
    val _ = Assert.assertAtomic' ("DmlCore.processSend(6)", SOME 1)
  in
    ()
  end

  fun processRecv {callerKind, channel = c, recvActAid, recvWaitNode} =
  let
    val _ = Assert.assertAtomic' ("DmlCore.processRecv(1)", SOME 1)
    val _ = debug' ("DmlCore.processRecv(1)")
    val recvActAid = aidIncVersion recvActAid
    val value =
      case PendingComm.deque pendingLocalSends c {againstAid = recvActAid} of
          NONE => (* No local matching sends, check remote *)
            let
              val _ = debug' ("DmlCore.processRecv(2)")
            in
              case PendingComm.deque pendingRemoteSends c {againstAid = recvActAid} of
                    NONE => (* No matching remote send either *)
                      let
                        val _ = debug' ("DmlCore.processRecv(3)")
                        val _ = msgSend (R_ACT {channel = c, recvActAid = recvActAid})
                        val _ = PendingComm.addAid pendingLocalRecvs c recvActAid
                          {recvWaitNode = recvWaitNode}
                        val value = case callerKind of
                                    Client => blockCurrentThread ()
                                  | Daemon => emptyW8Vec
                      in
                        value
                      end
                  | SOME (sendActAid, value) => (* matching remote send *)
                      let
                        val _ = debug' ("DmlCore.processRecv(4)")
                        val _ = msgSend (R_ACT {channel = c, recvActAid = recvActAid})
                        val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                        val _ = MatchedComm.add matchedRecvs {channel = c, actAid = recvActAid,
                                  remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
                        val _ = case callerKind of
                                  Client => S.atomicEnd ()
                                | Daemon => ()
                      in
                        value
                      end
            end
        | SOME (sendActAid, {sendWaitNode, value}) => (* matching local send *)
            let
              val _ = debug' ("DmlCore.processRecv(5)")
              val _ = setMatchAid sendWaitNode recvActAid value
              val _ = setMatchAid recvWaitNode sendActAid emptyW8Vec
              val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val () = case callerKind of
                          Daemon => resumeThreadIfLastAidIs (getNextAid recvActAid) (aidToTidInt recvActAid) value
                        | Client => S.atomicEnd ()
            in
              value
            end
    val _ = debug' ("DmlCore.processRecv(6)")
    val _ = case callerKind of
                 Client => Assert.assertNonAtomic' ("DmlCore.processRecv(2)")
               | Daemon => Assert.assertAtomic' ("DmlCore.processRecv(2)", SOME 1)
  in
    value
  end

  fun processRollbackMsg rollbackAids dfsStartAct =
    let
      val _ = dfsStartAct (* silence warning *)
      val _ = debug' ("processRollbackMsg")
      val _ = PTRDict.app (fn (k,a) => debug' (ptrToString k^":"^(Int.toString a))) rollbackAids
      (* Clean up pending acts *)
      val () = PendingComm.cleanup pendingLocalSends rollbackAids
      val () = PendingComm.cleanup pendingRemoteSends rollbackAids
      val () = PendingComm.cleanup pendingLocalRecvs rollbackAids
      val () = PendingComm.cleanup pendingRemoteRecvs rollbackAids
      (* Cleanup matched acts *)
      val failList = MatchedComm.cleanup matchedSends rollbackAids
      val _ = ListMLton.map (failList, fn {channel, actAid, waitNode, value} =>
                processSend {channel = channel, sendActAid = actAid,
                sendWaitNode = waitNode, value = value})
      val failList = MatchedComm.cleanup matchedRecvs rollbackAids
      val _ = ListMLton.map (failList, fn {channel, actAid, waitNode, value = _} =>
                processRecv {callerKind = Daemon, channel = channel,
                    recvActAid = actAid, recvWaitNode = waitNode})
      (* Add message filter *)
      val _ = MessageFilter.addToFilter rollbackAids
      (* rollback threads *)
      val _ = rollbackBlockedThreads rollbackAids
      val _ = rollbackReadyThreads rollbackAids
    in
      ()
    end

  fun forceCommit (node) =
  let
    val _ = debug' ("forceCommit")
    val _ = Assert.assertAtomic'("forceCommit(1)", NONE)
    val aid = (actionToAid o nodeToAction) node
    val _ = debug' ("forceCommit: "^(aidToString aid))
    val tidInt = aidToTidInt aid

    fun handleBlockedThread () =
    let
      val t = IntDict.lookup (!blockedThreads) tidInt
      val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
      fun prolog () = ((!commitRef) (); emptyW8Vec)
      val rt = S.prep (S.prepend (t, prolog))
    in
      S.ready rt
    end handle IntDict.Absent => handleReadyThread ()

    and handleReadyThread () =
    let
      fun core (rthrd as S.RTHRD (cmlTid, _)) =
      let
        val pid = ProcessId (!processId)
        val rid = CML.tidToRev cmlTid
        val tid = ThreadId (CML.tidToInt cmlTid)
      in
        if MLton.equal (aidToPtr aid, {pid = pid, tid = tid, rid = rid}) then
          S.RTHRD (cmlTid, MLton.Thread.prepare (MLton.Thread.new (!commitRef), ()))
        else rthrd
      end
    in
      S.modify core
    end

  in
    handleBlockedThread ()
  end

  fun updateRemoteChannels (ACTION {act, ...}, prev) =
    case prev of
         SOME (ACTION {act = SEND_ACT _, aid = sendActAid}) =>
           (case act of
              SEND_WAIT {cid, matchAid = SOME recvActAid} =>
                (debug' ("updateRemoteChannels(1): removing send "^(aidToString sendActAid));
                 PendingComm.removeAid pendingRemoteSends cid sendActAid;
                 debug' ("updateRemoteChannels(2): removing recv "^(aidToString recvActAid));
                 PendingComm.removeAid pendingRemoteRecvs cid recvActAid;
                 processSendJoin {channel = cid, sendActAid = sendActAid,
                                  recvActAid = recvActAid, ignoreFailure = false})
              | _ => raise Fail "updateRemoteChannels(1)")
       | SOME (ACTION {act = RECV_ACT _, aid = recvActAid}) =>
           (case act of
              RECV_WAIT {cid, matchAid = SOME sendActAid} =>
                (debug' ("updateRemoteChannels(2): removing send "^(aidToString sendActAid));
                 PendingComm.removeAid pendingRemoteSends cid sendActAid;
                 debug' ("updateRemoteChannels(2): removing recv "^(aidToString recvActAid));
                 PendingComm.removeAid pendingRemoteRecvs cid recvActAid;
                 processRecvJoin {channel = cid, sendActAid = sendActAid,
                                  recvActAid = recvActAid, ignoreFailure = false})
              | _ => raise Fail "updateRemoteChannels(2)")
       | _ => ()

  and processSendJoin {channel = c, sendActAid, recvActAid, ignoreFailure} =
      (debug' ("processSendJoin: ["^(aidToString sendActAid)^","^(aidToString recvActAid)^"]");
      if MessageFilter.isAllowed sendActAid then
        case MatchedComm.join matchedRecvs {remoteAid = sendActAid, withAid = recvActAid, ignoreFailure = ignoreFailure} of
            MatchedComm.NOOP => ()
          | MatchedComm.SUCCESS {value, waitNode = recvWaitNode} =>
              let
                val _ = setMatchAid recvWaitNode sendActAid value
                val tidInt = aidToTidInt recvActAid
                val recvWaitAid = getNextAid recvActAid
                val _ = resumeThreadIfLastAidIs recvWaitAid tidInt value
              in
                ()
              end
          | MatchedComm.FAILURE {actAid = recvActAid2, waitNode = recvWaitNode, ...} =>
            let
              val _ = if MLton.equal (aidToPtr recvActAid, aidToPtr recvActAid2) andalso
                          MLton.equal (ActionIdOrdered.compare (recvActAid2, recvActAid), LESS) then
                            msgSend (R_JOIN {channel = c, recvActAid = recvActAid, sendActAid = dummyAid})
                      else ()
            in
              if not (isLastNode recvWaitNode) then
                    (* Create a self-cycle for this node, which will cause it
                    * to fail on commit. Also, value is set to emptyW8Vec,
                    * which will (and should) never be deserialized to the
                    * type of recv result. *)
                    (msgSend (R_JOIN {channel = c, recvActAid = recvActAid2, sendActAid = recvActAid2});
                    debug' ("SUCCESS'");
                    forceCommit (recvWaitNode);
                    setMatchAid recvWaitNode (actionToAid (nodeToAction recvWaitNode)) emptyW8Vec)
              else
                ignore (processRecv {callerKind = Daemon, channel = c,
                          recvActAid = recvActAid2, recvWaitNode = recvWaitNode})
            end
      else ())

  and processRecvJoin {channel = c, sendActAid, recvActAid, ignoreFailure} =
      (debug' ("processRecvJoin: ["^(aidToString sendActAid)^","^(aidToString recvActAid)^"]");
      if MessageFilter.isAllowed recvActAid then
        case MatchedComm.join matchedSends {remoteAid = recvActAid, withAid = sendActAid, ignoreFailure = ignoreFailure} of
          MatchedComm.NOOP => ()
        | MatchedComm.SUCCESS {waitNode = sendWaitNode, ...} =>
            let
              val _ = setMatchAid sendWaitNode recvActAid emptyW8Vec
            in
              ()
            end
        | MatchedComm.FAILURE {actAid = sendActAid2, waitNode = sendWaitNode, value} =>
            let
              val _ = if MLton.equal (aidToPtr sendActAid, aidToPtr sendActAid2) andalso
                          MLton.equal (ActionIdOrdered.compare (sendActAid2, sendActAid), LESS) then
                            msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = dummyAid})
                      else ()
            in
              ignore (processSend {channel = c, sendActAid = sendActAid2,
                                   sendWaitNode = sendWaitNode, value = value})
            end
      else ())

  and processMsg msg =
  let
    val _ = Assert.assertAtomic' ("DmlCore.processMsg", SOME 1)
  in
    case msg of
        S_ACT {channel = c, sendActAid, value} =>
          if MessageFilter.isAllowed sendActAid andalso
             (not (MatchedComm.contains matchedRecvs sendActAid)) then
            (case PendingComm.deque pendingLocalRecvs c {againstAid = sendActAid} of
                NONE => (* No matching receives *)
                  PendingComm.addAid pendingRemoteSends c sendActAid value
              | SOME (recvActAid, {recvWaitNode}) => (* matching recv *)
                  let
                    val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                  in
                    MatchedComm.add matchedRecvs {channel = c, actAid = recvActAid,
                      remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
                  end)
          else ()
      | R_ACT {channel = c, recvActAid} =>
          if MessageFilter.isAllowed recvActAid andalso
             (not (MatchedComm.contains matchedSends recvActAid)) then
            (case PendingComm.deque pendingLocalSends c {againstAid = recvActAid} of
                NONE => (* No matching sends *)
                  PendingComm.addAid pendingRemoteRecvs c recvActAid ()
              | SOME (sendActAid, {sendWaitNode, value}) => (* matching send *)
                let
                  val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                in
                  MatchedComm.add matchedSends {channel = c, actAid = sendActAid,
                    remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
                end)
          else ()
      | S_JOIN {channel, sendActAid, recvActAid} =>
          processSendJoin {channel = channel, sendActAid = sendActAid,
                           recvActAid = recvActAid, ignoreFailure = true}
      | R_JOIN {channel, sendActAid, recvActAid} =>
          processRecvJoin {channel = channel, sendActAid = sendActAid,
                           recvActAid = recvActAid, ignoreFailure = true}
      | AR_RES_SUCC {dfsStartAct = _} => ()
          (* If you have the committed thread in your finalSatedComm structure, move to memoized *)
      | AR_RES_FAIL {dfsStartAct, rollbackAids} => processRollbackMsg rollbackAids dfsStartAct
      | AR_REQ_ADD {action as ACTION{aid, ...}, prevAction} =>
          if MessageFilter.isAllowed aid then
            (updateRemoteChannels (action, prevAction);
             processAdd {action = action, prevAction = prevAction})
          else ()
      | _ => ()
  end

  fun clientDaemon () =
    if (!exitDaemon) then ()
    else
      case msgRecvSafe () of
          NONE => (C.yield (); clientDaemon ())
        | SOME m =>
            let
              val _ = S.atomicBegin ()
              val _ = processMsg m
              val _ = S.atomicEnd ()
            in
              clientDaemon ()
            end

  (* -------------------------------------------------------------------- *)
  (* Client *)
  (* -------------------------------------------------------------------- *)

  val yield = C.yield

  fun connect {sink = sink_str, source = source_str, processId = pid, numPeers} =
  let
    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
    val _ = ZMQ.sockSetSubscribe (source, Vector.tabulate (0, fn _ => 0wx0))
    val _ = proxy := PROXY {context = SOME context, source = SOME source, sink = SOME sink}
    val _ = processId := pid

    val debug' = fn s => if false then debug' s else ()

    val _ = debug' ("DmlCore.connect.join(1)")
    fun join n =
    let
      val n = if n = 100000 then
                let
                  val _ = debug' ("DmlCore.connect.join: send CONN")
                  val _ = msgSendSafe (CONN {pid = ProcessId (!processId)})
                in
                  0
                end
              else n+1
      val () = case msgRecvSafe () of
                    NONE => join n
                  | SOME (CONN {pid = ProcessId pidInt}) =>
                      let
                        val _ = debug' ("DmlCore.connect.join(2)")
                        val _ = if ISS.member (!peers) pidInt then ()
                                else peers := ISS.insert (!peers) pidInt
                      in
                        if ISS.size (!peers) = (numPeers-1) then msgSendSafe (CONN {pid = ProcessId (!processId)})
                        else join n
                      end
                  | SOME m => raise Fail ("DmlCore.connect: unexpected message during connect" ^ (msgToString m))
    in
      ()
    end
    val _ = if numPeers = 1 then () else join 0
    (* If we get here, then we have joined *)
    val _ = debug' ("DmlCore.connect.join(3)")
  in
    ()
  end

  fun saveCont () = GraphManager.saveCont (fn () => ignore(insertRollbackNode ()))

  fun runDML (f, to) =
    let
      val _ = Assert.assert ([], fn () => "runDML must be run after connect",
                             fn () => case !proxy of
                                        PROXY {sink = NONE, ...} => false
                                      | _ => true)
      fun body () =
      let
        (* start the daemon. Insert a CR_RB node just so that functions which
         * expect a node at every tid will not throw exceptions. *)
        val _ = C.spawn (fn () =>
                  let
                    val _ = insertCommitNode ()
                  in
                    clientDaemon ()
                  end)
        val _ = insertCommitNode ()
        val _ = saveCont ()
        val _ = f ()
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

  (* Wait till last action is matched (added to the graph) *)
  fun syncMode (atomicState) =
    (if inNonSpecExecMode () andalso not (GraphManager.isLastNodeMatched ()) then
       let
         val _ = if MLton.equal (atomicState, NONE) then S.atomicBegin () else ()
         val {read = wait, write = wakeup} = IVar.new ()
         val _ = GraphManager.doOnUpdateLastNode wakeup
         val _ = S.atomicEnd ()
       in
         wait ()
       end
     else if MLton.equal (atomicState, SOME 1) then S.atomicEnd () else ();
     Assert.assertNonAtomic' ("syncMode(2)"))


  fun send (CHANNEL c, m) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlCore.send(1)")
  in
    case handleSend {cid = c} of
      UNCACHED {actAid, waitNode} =>
        let
          val m = MLton.serialize (m)
          val _ = processSend
            {channel = c, sendActAid = actAid,
            sendWaitNode = waitNode, value = m}
        in
          syncMode (SOME 1)
        end
    | CACHED _ => S.atomicEnd ()
  end

  fun recv (CHANNEL c) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlCore.recv(1)")
  in
    case handleRecv {cid = c} of
      UNCACHED {actAid, waitNode} =>
        let
          val serM = processRecv {callerKind = Client, channel = c,
                      recvActAid = actAid, recvWaitNode = waitNode}
          val result = MLton.deserialize serM
          val _ = syncMode (NONE)
        in
          result
        end
    | CACHED value =>
         (S.atomicEnd ();
          MLton.deserialize value)
  end

  val exitDaemon = fn () => exitDaemon := true

  fun commit () =
  let
    val finalAction = GraphManager.getFinalAction ()
    val {read, write} = IVar.new ()
    val _ = CML.spawn (fn () => processCommit {action = finalAction, pushResult = write})
    val _ = case read () of
                 AR_RES_SUCC _ => ()
               | AR_RES_FAIL {rollbackAids, dfsStartAct} =>
                   let
                     val _ = debug (fn () => "Commit Failure: size(rollbackAids)="^
                                   (Int.toString (PTRDict.size (rollbackAids))))
                     val () = S.atomicBegin ()
                     val () = processRollbackMsg rollbackAids dfsStartAct
                     val () = S.atomicEnd ()
                   in
                     restoreCont (PTRDict.lookup rollbackAids (aidToPtr (actionToAid finalAction)))
                   end
               | _ => raise Fail "DmlCore.commit: unexpected message"

    (* Committed Successfully *)
    val _ = debug' ("DmlCore.commit: SUCCESS")
    (* --- Racy code --- *)
    val _ = CML.tidCommit (S.getCurThreadId ())
    (* --- End racy code --- *)
    val _ = insertCommitNode ()
    val _ = saveCont ()
    val _ = Assert.assertNonAtomic' ("DmlCore.commit")
  in
    ()
  end

  val _ = commitRef := commit

  fun spawn f =
    let
      val _ = commit ()
      val tid = S.newTid ()
      val tidInt = C.tidToInt tid
      val {spawnAid, spawnNode = _}= handleSpawn {childTid = ThreadId tidInt}
      fun prolog () =
        let
          val _ = S.atomicBegin ()
          val _ = handleInit {parentAid = spawnAid}
          val _ = S.atomicEnd ()
        in
          saveCont ()
        end
      val _ = ignore (C.spawnWithTid (f o prolog, tid))
      val _ = commit ()
    in
      ()
    end

  fun getThreadId () =
  let
    val pidInt = !processId
    val tidInt = S.tidInt ()
  in
    {pid = pidInt, tid = tidInt}
  end

  fun touchLastComm () =
  let
    val _ = S.atomicBegin ()
  in
    if not (GraphManager.isLastNodeMatched ()) then
      let
        val {read = wait, write = wakeup} = IVar.new ()
        val _ = GraphManager.doOnUpdateLastNode wakeup
        val _ = S.atomicEnd ()
      in
        wait ()
      end
    else S.atomicEnd ()
  end
end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
