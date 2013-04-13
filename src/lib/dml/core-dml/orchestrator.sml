(* Copyright (C) 2013 KC Sivaramakrishnan.
:q
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure Orchestrator : ORCHESTRATOR =
struct
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  open RepTypes
  open ActionHelper
  open CommunicationHelper
  open GraphManager
  open CycleDetector

  structure S = CML.Scheduler
  structure C = CML
  structure ISS = IntSplaySet
  structure ZMQ = MLton.ZMQ
  structure SH = SchedulerHelper

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)


  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val pendingLocalSends : {sendWaitNode : node, value : w8vec} PendingComm.t = PendingComm.empty ()
  val pendingLocalRecvs : {recvWaitNode : node} PendingComm.t = PendingComm.empty ()
  val pendingRemoteSends : w8vec PendingComm.t = PendingComm.empty ()
  val pendingRemoteRecvs : unit PendingComm.t = PendingComm.empty ()

  val matchedSends : w8vec MatchedComm.t = MatchedComm.empty ()
  val matchedRecvs : w8vec MatchedComm.t = MatchedComm.empty ()

  (* State for join and exit*)
  val peers = ref (ISS.empty)
  val exitDaemon = ref false

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

  fun cleanPending actAid waitNode =
    case nodeToAction waitNode of
         BASE _ => ()
       | EVENT {actions} =>
           let
             val diff = AidDict.size actions
             val actions = AidDict.remove actions actAid
           in
            AidDict.app (fn (aid, act) =>
              case act of
                  SEND_WAIT {cid, ...} => ignore (PendingComm.removeAid pendingLocalSends cid (actNumMinus aid diff))
                | RECV_WAIT {cid, ...} => ignore (PendingComm.removeAid pendingLocalRecvs cid (actNumMinus aid diff))
                | _ => raise Fail "cleanPending") actions
           end

  val _ = GraphManager.cleanPending := cleanPending

  fun processSend {callerKind = _, channel = c, sendActAid, sendWaitNode, value} =
  let
    val _ = Assert.assertAtomic' ("DmlCore.processSend(1)", SOME 1)
    val _ = debug' ("DmlCore.processSend(1)")
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
                        val _ = msgSend (S_MATCH {channel = c, sendActAid = sendActAid, recvActAid = recvActAid, value = value})
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
              val _ = setMatchAid {waitNode = sendWaitNode, actAid = sendActAid,
                                   matchAid = recvActAid, value = value}
              val _ = setMatchAid {waitNode = recvWaitNode, actAid = recvActAid,
                                   matchAid = sendActAid, value = emptyW8Vec}
              val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
            in
              (* Resume blocked recv *)
              if isLastNode recvWaitNode then
                SH.resumeThread (aidToTidInt recvActAid) value
              else ()
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
                                    Client => SH.blockCurrentThread ()
                                  | Daemon => emptyW8Vec
                      in
                        value
                      end
                  | SOME (sendActAid, value) => (* matching remote send *)
                      let
                        val _ = debug' ("DmlCore.processRecv(4)")
                        val _ = msgSend (R_MATCH {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
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
              val _ = setMatchAid {waitNode = sendWaitNode, actAid = sendActAid,
                                   matchAid = recvActAid, value = value}
              val _ = setMatchAid {waitNode = recvWaitNode, actAid = recvActAid,
                                   matchAid = sendActAid, value = emptyW8Vec}
              val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val () = case callerKind of
                          Daemon => if isLastNode recvWaitNode then
                                      SH.resumeThread (aidToTidInt recvActAid) value
                                    else ()
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
                processSend {callerKind = Daemon, channel = channel,
                sendActAid = actAid, sendWaitNode = waitNode, value = value})
      val failList = MatchedComm.cleanup matchedRecvs rollbackAids
      val _ = ListMLton.map (failList, fn {channel, actAid, waitNode, value = _} =>
                processRecv {callerKind = Daemon, channel = channel,
                recvActAid = actAid, recvWaitNode = waitNode})

      (* Add message filter *)
      val _ = MessageFilter.addToFilter rollbackAids
      (* rollback threads *)
      val _ = SH.rollbackBlockedThreads rollbackAids
      val _ = SH.rollbackReadyThreads rollbackAids
    in
      ()
    end

  fun cleanRemoteChannels (actions) =
    AidDict.app (fn (aid, act) =>
      case act of
           SEND_ACT {cid} =>
             (ignore (PendingComm.removeAid pendingRemoteSends cid aid);
              processSendJoin {channel = cid, sendActAid = aid,
                               recvActAid = dummyAid, ignoreFailure = false})
         | RECV_ACT {cid} =>
             (ignore (PendingComm.removeAid pendingRemoteRecvs cid aid);
              processRecvJoin {channel = cid, recvActAid = aid,
                               sendActAid = dummyAid, ignoreFailure = false})
         | _ => raise Fail "cleanRemoteChannels") actions

   and updateRemoteChannels (act, prev) =
    case prev of
         SOME (BASE {act = SEND_ACT _, aid = sendActAid}) =>
           (case act of
              SEND_WAIT {cid, matchAid = SOME recvActAid} =>
                (debug' ("updateRemoteChannels(1): removing send "^(aidToString sendActAid));
                 ignore (PendingComm.removeAid pendingRemoteSends cid sendActAid);
                 debug' ("updateRemoteChannels(2): removing recv "^(aidToString recvActAid));
                 ignore (PendingComm.removeAid pendingRemoteRecvs cid recvActAid);
                 processSendJoin {channel = cid, sendActAid = sendActAid,
                                  recvActAid = recvActAid, ignoreFailure = false})
              | _ => raise Fail "updateRemoteChannels(1)")
       | SOME (BASE {act = RECV_ACT _, aid = recvActAid}) =>
           (case act of
              RECV_WAIT {cid, matchAid = SOME sendActAid} =>
                (debug' ("updateRemoteChannels(2): removing send "^(aidToString sendActAid));
                 ignore (PendingComm.removeAid pendingRemoteSends cid sendActAid);
                 debug' ("updateRemoteChannels(2): removing recv "^(aidToString recvActAid));
                 ignore (PendingComm.removeAid pendingRemoteRecvs cid recvActAid);
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
                val _ = setMatchAid {waitNode = recvWaitNode, actAid = recvActAid,
                                     matchAid = sendActAid, value = value}
              in
                if isLastNode recvWaitNode then
                  SH.resumeThread (aidToTidInt recvActAid) value
                else ()
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
                    (SH.forceCommit (recvWaitNode);
                     setMatchAid {waitNode = recvWaitNode, actAid = recvActAid, value = emptyW8Vec,
                                  matchAid = getWaitAid {actAid = recvActAid, waitNode = recvWaitNode}};
                     debug' ("SUCCESS'"))
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
              val _ = setMatchAid {waitNode = sendWaitNode, actAid = sendActAid,
                                   matchAid = recvActAid, value = emptyW8Vec}
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
              ignore (processSend {callerKind = Daemon, channel = c, sendActAid = sendActAid2,
                                   sendWaitNode = sendWaitNode, value = value})
            end
      else ())

  and processSendMatch {channel = c, sendActAid (* remote *), recvActAid (* local *), value} =
  (debug' ("processSendMatch: ["^(aidToString sendActAid)^","^(aidToString recvActAid)^"]");
  if MessageFilter.isAllowed sendActAid andalso
     (not (isMatched recvActAid)) then
   if not (aidToPidInt recvActAid = (!processId)) then
     processMsg (S_ACT {channel = c, sendActAid = sendActAid, value = value})
   else if MatchedComm.contains matchedRecvs sendActAid then
     processSendJoin {channel = c, sendActAid = sendActAid, recvActAid = recvActAid, ignoreFailure = false}
   else if not (PendingComm.contains pendingLocalRecvs c recvActAid) then
     processMsg (S_ACT {channel = c, sendActAid = sendActAid, value = value})
   else
     let
       val {recvWaitNode} = valOf (PendingComm.removeAid pendingLocalRecvs c recvActAid)
       val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
       val _ = MatchedComm.add matchedRecvs {channel = c, actAid = recvActAid,
                 remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
     in
       ()
     end
   else ())

  and processRecvMatch {channel = c, sendActAid (* local *), recvActAid (* remote *)} =
  (debug' ("processRecvMatch: ["^(aidToString recvActAid)^","^(aidToString sendActAid)^"]");
  if MessageFilter.isAllowed recvActAid andalso
     (not (isMatched sendActAid)) then
   if not (aidToPidInt sendActAid = (!processId)) then
     processMsg (R_ACT {channel = c, recvActAid = recvActAid})
   else if MatchedComm.contains matchedSends recvActAid then
     processRecvJoin {channel = c, sendActAid = sendActAid, recvActAid = recvActAid, ignoreFailure = false}
   else if not (PendingComm.contains pendingLocalSends c sendActAid) then
     processMsg (R_ACT {channel = c, recvActAid = recvActAid})
   else
     let
       val {sendWaitNode, value} = valOf (PendingComm.removeAid pendingLocalSends c sendActAid)
       val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
       val _ = MatchedComm.add matchedSends {channel = c, actAid = sendActAid,
                 remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
     in
       ()
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
      | R_MATCH m => processRecvMatch m
      | S_MATCH m => processSendMatch m
      | AR_RES_SUCC {dfsStartAct = _} => ()
          (* If you have the committed thread in your finalSatedComm structure, move to memoized *)
      | AR_RES_FAIL {dfsStartAct, rollbackAids} => processRollbackMsg rollbackAids dfsStartAct
      | AR_REQ_ADD {action as BASE {aid, act}, prevAction} =>
          if MessageFilter.isAllowed aid then
            (updateRemoteChannels (act, prevAction);
             processAdd {action = action, prevAction = prevAction})
          else ()
      | AR_REQ_ADD {action = EVENT _, prevAction = _} => raise Fail "processMsg.AR_REQ_ADD: found EVENT"
      | CONN _ => ()
      | CLEAN {actions} => cleanRemoteChannels actions
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
end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
