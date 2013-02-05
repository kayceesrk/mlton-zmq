(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature PENDING_COMM =
sig
  type 'a t
  val empty     : unit -> 'a t
  val addAid    : 'a t -> RepTypes.channel_id -> ActionManager.action_id -> 'a -> unit
  val removeAid : 'a t -> RepTypes.channel_id -> ActionManager.action_id -> unit
  val deque     : 'a t -> RepTypes.channel_id -> {againstAid: ActionManager.action_id} -> (ActionManager.action_id * 'a) option
  val cleanup   : 'a t -> int RepTypes.PTRDict.dict -> unit
end

signature MATCHED_COMM =
sig
  type 'a t
  datatype 'a join_result =
    SUCCESS of {value : 'a, waitNode: POHelper.node}
  | FAILURE of {actAid : ActionManager.action_id,
                waitNode : POHelper.node,
                value : 'a}
  | NOOP

  val empty : unit -> 'a t
  val add   : 'a t -> {actAid: ActionManager.action_id,
                       remoteMatchAid: ActionManager.action_id,
                       waitNode: POHelper.node} -> 'a -> unit
  val join  : 'a t -> {remoteAid: ActionManager.action_id,
                       withAid: ActionManager.action_id} -> 'a join_result
end

signature SATED_COMM =
sig
  type t

  val empty : unit -> t
  val waitTillSated      : t -> ActionManager.action_id -> unit
  val addSatedAct        : t -> ActionManager.action_id -> POHelper.node -> unit
  val forceAddSatedAct   : t -> ActionManager.action_id -> unit
  val addSatedComm       : t -> {aid: ActionManager.action_id,
                                 matchAid: ActionManager.action_id,
                                 node: POHelper.node,
                                 value: RepTypes.w8vec option} -> unit
  val handleSatedMessage : t -> {recipient: RepTypes.process_id,
                                 remoteAid: ActionManager.action_id,
                                 matchAid: ActionManager.action_id} -> unit
end

structure DmlDecentralized : DML_INTERNAL =
struct
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  open RepTypes
  open ActionManager
  open CommunicationManager
  open POHelper
  open Arbitrator

  structure IntDict = IntSplayDict
  structure StrDict = StringSplayDict
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
  (* Pending Communication Helper *)
  (* -------------------------------------------------------------------- *)

  structure PendingComm : PENDING_COMM =
  struct
    open StrDict

    type 'a t = 'a AISD.dict StrDict.dict ref

    fun empty () = ref (StrDict.empty)

    fun addAid strDictRef (ChannelId channel) aid value =
    let
      fun merge oldAidDict = AISD.insert oldAidDict aid value
    in
      strDictRef := StrDict.insertMerge (!strDictRef) channel (AISD.singleton aid value) merge
    end

    fun removeAid strDictRef (ChannelId channel) aid =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      val aidDict = AISD.remove aidDict aid
    in
      strDictRef := StrDict.insert (!strDictRef) channel aidDict
    end handle StrDict.Absent => ()

    exception FIRST of action_id

    fun deque strDictRef (ChannelId channel) {againstAid} =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      fun getOne () =
      let
        val _ = AISD.app (fn (k, _) =>
                  if (aidToTidInt k = aidToTidInt againstAid) andalso
                     (aidToPidInt k = aidToPidInt againstAid)
                  then () (* dont match actions from the same thread *)
                  else raise FIRST k) aidDict
      in
        raise AISD.Absent
      end handle FIRST k => k
      val aid = getOne ()
      val return = SOME (aid, AISD.lookup aidDict aid)
      val _ = removeAid strDictRef (ChannelId channel) aid
    in
      return
    end handle AISD.Absent => NONE
             | StrDict.Absent => NONE

    fun cleanup strDictRef rollbackAids =
    let
      val _ = Assert.assertAtomic' ("PendingComm.cleanup", SOME 1)
      val oldStrDict = !strDictRef
      fun getNewAidDict oldAidDict =
        let
          val emptyAidDict = AISD.empty
        in
          AISD.foldl (fn (aid as ACTION_ID {pid, tid, rid, ...}, value, newAidDict) =>
            case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
                NONE => AISD.insert newAidDict aid value
              | SOME _ => newAidDict) emptyAidDict oldAidDict
        end
      val newStrDict = StrDict.map (fn aidDict => getNewAidDict aidDict) oldStrDict
    in
      strDictRef := newStrDict
    end
  end

  (* -------------------------------------------------------------------- *)
  (* Matched Communication Helper *)
  (* -------------------------------------------------------------------- *)

  structure MatchedComm : MATCHED_COMM =
  struct
    type 'a t = {actAid : action_id, waitNode : node, value : 'a} AISD.dict ref

    datatype 'a join_result =
      SUCCESS of {value : 'a, waitNode: POHelper.node}
    | FAILURE of {actAid : ActionManager.action_id,
                  waitNode : POHelper.node,
                  value : 'a}
    | NOOP

    fun empty () = ref (AISD.empty)

    fun add aidDictRef {actAid, remoteMatchAid, waitNode} value =
    let
      val _ = if isAidLocal remoteMatchAid then raise Fail "MatchedComm.add(1)"
              else if not (isAidLocal actAid) then raise Fail "MatchedComm.add(2)"
              else ()
    in
      aidDictRef := AISD.insert (!aidDictRef) remoteMatchAid
                    {actAid = actAid, waitNode = waitNode, value = value}
    end

    fun join aidDictRef {remoteAid, withAid} =
    let
      val _ = if isAidLocal remoteAid then raise Fail "MatchedComm.join"
              else ()
      val {actAid, waitNode, value} = AISD.lookup (!aidDictRef) remoteAid
      val result =
        if MLton.equal (actAid, withAid) then
          let
            val _ = debug' ("SUCCESS")
          in
            SUCCESS {value = value, waitNode = waitNode}
          end
        else
          let
            val _ = debug' ("FAILURE")
          in
            FAILURE {actAid = actAid, waitNode = waitNode, value = value}
          end
      val _ = aidDictRef := AISD.remove (!aidDictRef) remoteAid
    in
      result
    end handle AISD.Absent => (debug' ("NOOP"); NOOP)

  end



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
  val allThreads = ref (IntDict.empty)

  (* State for join and exit*)
  val peers = ref (ISS.empty)
  val exitDaemon = ref false

  (* -------------------------------------------------------------------- *)
  (* AllThreads Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun addToAllThreads () =
  let
    val _ = S.atomicBegin ()
    val newAllThreads = IntDict.insert (!allThreads) (S.tidInt ()) (S.getCurThreadId ())
    val _ =  allThreads := newAllThreads
  in
    S.atomicEnd ()
  end

  fun removeFromAllThreads () =
  let
    val _ = S.atomicBegin ()
    val newAllThreads = IntDict.remove (!allThreads) (S.tidInt ())
    val _ = allThreads := newAllThreads
  in
    S.atomicEnd ()
  end

  fun tid2tid (ThreadId tid) = IntDict.find (!allThreads) tid

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
    val _ = Assert.assertAtomic' ("DmlDecentralized.unblockthread", SOME 1)
    val t = IntDict.lookup (!blockedThreads) tidInt
    val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
    val rt = S.prepVal (t, value)
  in
    S.ready rt
  end handle IntDict.Absent => ()

  fun rollbackBlockedThreads ptrDict =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.rollbackBlockedThreads", SOME 1)
    fun rollbackBlockedThread (t as S.THRD (tid,_)) =
      let
        val prolog = fn () => (POHelper.restoreCont (); emptyW8Vec)
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
              | SOME _ => (rollbackBlockedThread t; newBTDict)
        end) IntDict.empty (!blockedThreads)
  in
    blockedThreads := newBTDict
  end

  fun rollbackReadyThreads ptrDict =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.rollbackReadyThreads", SOME 1)
    fun restoreSCore (rthrd as S.RTHRD (cmlTid, t)) =
      let
        val pid = ProcessId (!processId)
        val rid = CML.tidToRev cmlTid
        val tid = ThreadId (CML.tidToInt cmlTid)
      in
        case PTRDict.find ptrDict {pid = pid, tid = tid, rid = rid} of
             NONE => rthrd
           | SOME _ => S.RTHRD (cmlTid, MLton.Thread.prepare (MLton.Thread.new (restoreCont), ()))
      end
  in
    S.modify restoreSCore
  end

  (* -------------------------------------------------------------------- *)
  (* Sated Communications -- Callback support *)
  (* -------------------------------------------------------------------- *)

  structure SatedComm : SATED_COMM =
  struct
    structure Debug = LocalDebug(val debug = true)

    fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
    (* fun debug' msg = debug (fn () => msg) *)

    datatype t = SC of {waiting : thread_id AISD.dict ref, (* waiting threads *)
                        pending : {peer: {matchAid: action_id, value: w8vec option} option, node: POHelper.node} AISD.dict ref, (* matched but unsated actions *)
                        final   : {matchAid: action_id, value: w8vec option} option AISD.dict ref}

    fun empty () = SC {waiting = ref AISD.empty,
                       pending = ref AISD.empty,
                       final   = ref AISD.empty}

    fun waitTillSated (SC {final, waiting, ...}) onAid =
    let
      val _ = Assert.assertNonAtomic' ("SatedComm.waitTillSated(1)")
      val _ = S.atomicBegin ()
    in
      if AISD.member (!final) onAid then S.atomicEnd ()
      else
        let
          val tidInt = S.tidInt ()
          val _ = waiting := AISD.insert (!waiting) onAid (ThreadId tidInt)
        in
          ignore (blockCurrentThread ())
        end
    end

    fun maybeWakeupThread (SC {waiting, ...}) aid =
      case AISD.find (!waiting) aid of
           NONE => ()
         | SOME (ThreadId tidInt) =>
             (resumeThread tidInt emptyW8Vec;
              waiting := AISD.remove (!waiting) aid)

    fun maybeProcessPending (state as SC {pending, ...}) nextAid =
      case AISD.find (!pending) nextAid of
           NONE => ()
         | SOME kind =>
             let
               val _ = pending := AISD.remove (!pending) nextAid
               val _ = debug (fn () => "Add to pending "^(aidToString nextAid))
             in
               case kind of
                    {peer = NONE, node} => addSatedAct state nextAid node
                  | {peer = SOME {matchAid, value}, node} => addSatedComm state {aid = nextAid, matchAid = matchAid, node = node, value = value}
             end

    and addSatedAct (state as SC {final, pending, ...}) aid node =
    let
      val _ = Assert.assertAtomic' ("SatedComm.addSatedAct", SOME 1)
    in
      if AISD.member (!final) (getPrevAid aid) then
        let
          val _ = final := AISD.insert (!final) aid NONE
          val _ = POHelper.handleFinalizingSatedNode node
          val _ = debug (fn () => "Add to final "^(aidToString aid))
          val _ = maybeWakeupThread state aid
          val _ = maybeProcessPending state (getNextAid aid)
        in
          ()
        end
      else
        let
          val _ = pending := AISD.insert (!pending) aid {peer = NONE, node = node}
          val _ = debug (fn () => "Add to pending "^(aidToString aid))
        in
          ()
        end
    end

    and forceAddSatedAct (state as SC {final, ...}) aid =
    let
      val _ = S.atomicBegin ()
      val _ = Assert.assertAtomic' ("SatedComm.forceAddSatedAct", NONE)
      val _ = final := AISD.insert (!final) aid NONE
      val _ = debug (fn () => "Add to final "^(aidToString aid))
      val _ = maybeWakeupThread state aid
      val _ = maybeProcessPending state (getNextAid aid)
    in
      S.atomicEnd ()
    end

    and addSatedComm (state as SC {final, pending, ...}) {aid, matchAid, value, node} =
    let
      val _ = Assert.assertAtomic' ("SatedComm.addSatedComm", SOME 1)
    in
      (* prevAct, matchAct \in final *)
      if AISD.member (!final) (getPrevAid aid) andalso AISD.member (!final) matchAid then
        let
          val _ = final := AISD.insert (!final) aid (SOME {matchAid = matchAid, value = value})
          val _ = POHelper.handleFinalizingSatedNode node
          val _ = debug (fn () => "Add to final "^(aidToString aid))
          val _ = maybeWakeupThread state aid
          val _ = maybeProcessPending state (getNextAid aid)
        in
          ()
        end
      (* prevAct \in final, matchAct \notin final, remote(matchAid) *)
      else if AISD.member (!final) (getPrevAid aid) andalso not (isAidLocal matchAid) then
        let
          val _ = msgSend (SATED {recipient = ProcessId (aidToPidInt matchAid),
                                  remoteAid = aid, matchAid = matchAid})
          val _ = pending := AISD.insert (!pending) aid {peer = SOME {matchAid = matchAid, value = value}, node = node}
          val _ = debug (fn () => "Add to pending "^(aidToString aid))
        in
          ()
        end
      (* prevAct \in final, matchAct \in pending, local(matchAid) *)
      else if AISD.member (!final) (getPrevAid aid) andalso AISD.member (!pending) matchAid andalso (isAidLocal matchAid) then
        let
          val _ = final := AISD.insert (!final) aid (SOME {matchAid = matchAid, value = value})
          val _ = POHelper.handleFinalizingSatedNode node
          val _ = debug (fn () => "Add to final "^(aidToString aid))
          val _ = maybeProcessPending state matchAid
        in
          ()
        end
      (* prevAct \notin final *)
      else
        let
          val _ = pending := AISD.insert (!pending) aid {peer = SOME {matchAid = matchAid, value = value}, node = node}
          val _ = debug (fn () => "Add to pending "^(aidToString aid))
        in
          ()
        end
    end

    and handleSatedMessage (state as SC {final, ...}) {recipient = _, remoteAid, matchAid} =
    let
      val _ = Assert.assertAtomic' ("SatedComm.handleSatedMessage", SOME 1)
      val _ = final := AISD.insert (!final) remoteAid NONE
      val _ = debug (fn () => "Add to final "^(aidToString remoteAid))
    in
      maybeProcessPending state matchAid
    end

  end (* structure SatedComm *)

  val satedCommHelper = SatedComm.empty ()

  fun resumeBlockedRecv {sendActAid, recvActAid, recvWaitNode} value =
  let
    val tidInt = aidToTidInt recvActAid
    val _ = SatedComm.addSatedComm satedCommHelper
             {aid = recvActAid, matchAid = sendActAid, node = getPrevNode recvWaitNode, value = SOME value}
    val _ = SatedComm.addSatedAct satedCommHelper (getNextAid recvActAid) recvWaitNode
  in
    resumeThread tidInt value
  end

  fun satiateSend {sendActAid, recvActAid, sendWaitNode} =
  let
    val _ = SatedComm.addSatedComm satedCommHelper
              {aid = sendActAid, matchAid = recvActAid, node = getPrevNode sendWaitNode, value = NONE}
    val _ = SatedComm.addSatedAct satedCommHelper (getNextAid sendActAid) sendWaitNode
  in
    ()
  end

  fun satiateRecv {sendActAid, recvActAid, recvWaitNode} value =
  let
    val _ = SatedComm.addSatedComm satedCommHelper
              {aid = recvActAid, matchAid = sendActAid, node = getPrevNode recvWaitNode, value = SOME value}
    val _ = SatedComm.addSatedAct satedCommHelper (getNextAid recvActAid) recvWaitNode
  in
    ()
  end


  (* -------------------------------------------------------------------- *)
  (* Server *)
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

  fun processLocalSend callerKind {channel = c, sendActAid, sendWaitNode, value} =
  let
    val _ = callerKind
    val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(1)", SOME 1)
    val _ = debug' ("DmlDecentralized.processLocalSend(1)")
    val _ =
      case PendingComm.deque pendingLocalRecvs c {againstAid = sendActAid} of
          NONE => (* No matching receives, check remote *)
            let
              val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(2)", SOME 1)
              val _ = debug' ("DmlDecentralized.processLocalSend(2)")
            in
              case PendingComm.deque pendingRemoteRecvs c {againstAid = sendActAid} of
                    NONE => (* No matching remote recv either *)
                      let
                        val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(3)", SOME 1)
                        val _ = debug' ("DmlDecentralized.processLocalSend(3)")
                        val _ = msgSend (S_ACT {channel = c, sendActAid = sendActAid, value = value})
                        val _ = PendingComm.addAid pendingLocalSends c sendActAid
                          {sendWaitNode = sendWaitNode, value = value}
                      in
                        ()
                      end
                  | SOME (recvActAid, ()) => (* matching remote recv *)
                      let
                        val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(4)", SOME 1)
                        val _ = debug' ("DmlDecentralized.processLocalSend(4)")
                        val _ = msgSend (S_ACT {channel = c, sendActAid = sendActAid, value = value})
                        val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                        val _ = MatchedComm.add matchedSends
                          {actAid = sendActAid, remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
                      in
                        ()
                      end
            end
        | SOME (recvActAid, {recvWaitNode}) => (* matching local recv *)
            let
              val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(5)", SOME 1)
              val _ = debug' ("DmlDecentralized.processLocalSend(5)")
              val _ = setMatchAid sendWaitNode recvActAid
              val _ = setMatchAid recvWaitNode sendActAid
              val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = resumeBlockedRecv {recvWaitNode = recvWaitNode, sendActAid = sendActAid, recvActAid = recvActAid} value
              val _ = satiateSend {sendWaitNode = sendWaitNode, sendActAid = sendActAid, recvActAid = recvActAid}
            in
              ()
            end
    val _ = debug' ("DmlDecentralized.processLocalSend(6)")
    val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(6)", SOME 1)
  in
    ()
  end

  fun processLocalRecv callerKind {channel = c, recvActAid, recvWaitNode} =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalRecv(1)", SOME 1)
    val _ = debug' ("DmlDecentralized.processLocalRecv(1)")
    val value =
      case PendingComm.deque pendingLocalSends c {againstAid = recvActAid} of
          NONE => (* No local matching sends, check remote *)
            let
              val _ = debug' ("DmlDecentralized.processLocalRecv(2)")
            in
              case PendingComm.deque pendingRemoteSends c {againstAid = recvActAid} of
                    NONE => (* No matching remote send either *)
                      let
                        val _ = debug' ("DmlDecentralized.processLocalRecv(3)")
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
                        val _ = debug' ("DmlDecentralized.processLocalRecv(4)")
                        val _ = msgSend (R_ACT {channel = c, recvActAid = recvActAid})
                        val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                        val _ = MatchedComm.add matchedRecvs
                          {actAid = recvActAid, remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
                        val value = case callerKind of
                                    Client => blockCurrentThread ()
                                  | Daemon => value
                      in
                        value
                      end
            end
        | SOME (sendActAid, {sendWaitNode, value}) => (* matching local send *)
            let
              val _ = debug' ("DmlDecentralized.processLocalRecv(5)")
              val _ = setMatchAid sendWaitNode recvActAid
              val _ = setMatchAid recvWaitNode sendActAid
              val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = satiateSend {sendWaitNode = sendWaitNode, sendActAid = sendActAid, recvActAid = recvActAid}
              val () = case callerKind of
                          Daemon => resumeBlockedRecv {recvWaitNode = recvWaitNode, sendActAid = sendActAid, recvActAid = recvActAid} value
                        | Client =>
                            let
                              val _ = satiateRecv {sendActAid = sendActAid, recvActAid = recvActAid, recvWaitNode = recvWaitNode} value
                            in
                              S.atomicEnd ()
                            end
            in
              value
            end
    val _ = debug' ("DmlDecentralized.processLocalRecv(6)")
    val _ = case callerKind of
                 Client => Assert.assertNonAtomic' ("DmlDecentralized.processLocalRecv(2)")
               | Daemon => Assert.assertAtomic' ("DmlDecentralized.processLocalRecv(2)", SOME 1)
  in
    value
  end

  fun processRollbackMsg rollbackAids =
    let
      val _ = PendingComm.cleanup pendingLocalSends rollbackAids
      val _ = PendingComm.cleanup pendingRemoteSends rollbackAids
      val _ = PendingComm.cleanup pendingLocalRecvs rollbackAids
      val _ = PendingComm.cleanup pendingRemoteRecvs rollbackAids
      val _ = rollbackBlockedThreads rollbackAids
      val _ = rollbackReadyThreads rollbackAids
    in
      ()
    end


  fun processMsg msg =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.processMsg", SOME 1)
  in
    case msg of
        S_ACT {channel = c, sendActAid, value} =>
        let
          val _ = Assert.assert ([], fn () => "DmlDecentralized.processMsg: remote S_ACT",
                                 fn () => not (isAidLocal sendActAid))
        in
          (case PendingComm.deque pendingLocalRecvs c {againstAid = sendActAid} of
              NONE => (* No matching receives *)
                PendingComm.addAid pendingRemoteSends c sendActAid value
            | SOME (recvActAid, {recvWaitNode}) => (* matching recv *)
                let
                  val _ = msgSend (R_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
                in
                  MatchedComm.add matchedRecvs
                    {actAid = recvActAid, remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
                end)
        end
      | R_ACT {channel = c, recvActAid} =>
        let
          val _ = Assert.assert ([], fn () => "DmlDecentralized.processMsg: remote R_ACT",
                                 fn () => not (isAidLocal recvActAid))
        in
          (case PendingComm.deque pendingLocalSends c {againstAid = recvActAid} of
              NONE => (* No matching sends *)
                PendingComm.addAid pendingRemoteRecvs c recvActAid ()
            | SOME (sendActAid, {sendWaitNode, value}) => (* matching send *)
               let
                 val _ = msgSend (S_JOIN {channel = c, sendActAid = sendActAid, recvActAid = recvActAid})
               in
                MatchedComm.add matchedSends
                  {actAid = sendActAid, remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
               end)
        end
      | S_JOIN {channel = c, sendActAid, recvActAid} =>
          let
            val _ = PendingComm.removeAid pendingLocalSends c sendActAid
            val _ = PendingComm.removeAid pendingRemoteSends c sendActAid
            val _ = PendingComm.removeAid pendingLocalRecvs c recvActAid
            val _ = PendingComm.removeAid pendingRemoteRecvs c recvActAid
          in
            case MatchedComm.join matchedRecvs {remoteAid = sendActAid, withAid = recvActAid} of
                MatchedComm.NOOP => ()
              | MatchedComm.SUCCESS {value, waitNode = recvWaitNode} =>
                  let
                    val _ = setMatchAid recvWaitNode sendActAid
                  in
                    resumeBlockedRecv {recvWaitNode = recvWaitNode, sendActAid = sendActAid, recvActAid = recvActAid} value
                  end
              | MatchedComm.FAILURE {actAid = recvActAid, waitNode = recvWaitNode, ...} =>
                    ignore (processLocalRecv Daemon {channel = c, recvActAid = recvActAid, recvWaitNode = recvWaitNode})
          end
      | R_JOIN {channel = c, recvActAid, sendActAid} =>
          let
            val _ = PendingComm.removeAid pendingLocalSends c sendActAid
            val _ = PendingComm.removeAid pendingRemoteSends c sendActAid
            val _ = PendingComm.removeAid pendingLocalRecvs c recvActAid
            val _ = PendingComm.removeAid pendingRemoteRecvs c recvActAid
          in
            case MatchedComm.join matchedSends {remoteAid = recvActAid, withAid = sendActAid} of
               MatchedComm.NOOP => ()
             | MatchedComm.SUCCESS {waitNode = sendWaitNode, ...} =>
                 let
                   val _ = setMatchAid sendWaitNode recvActAid
                 in
                   satiateSend {sendWaitNode = sendWaitNode, sendActAid = sendActAid, recvActAid = recvActAid}
                 end
             | MatchedComm.FAILURE {actAid = sendActAid, waitNode = sendWaitNode, value} =>
                   ignore (processLocalSend Daemon {channel = c, sendActAid = sendActAid, sendWaitNode = sendWaitNode, value = value})
          end
      | SATED (m as {recipient = ProcessId pidInt, ...}) =>
          if not (pidInt = !processId) then ()
          else
            SatedComm.handleSatedMessage satedCommHelper m
      | AR_RES_SUCC {aid} =>
          resumeThread (aidToTidInt aid) emptyW8Vec
          (* If you have the committed thread in your finalSatedComm structure, move to memoized *)
      | AR_RES_FAIL {rollbackAids} => processRollbackMsg rollbackAids
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
    (* If we get here, then we have joined *)
    val _ = debug' ("DmlDecentralized.connect.join(3)")
  in
    ()
  end

  fun saveCont () = POHelper.saveCont (fn () => SatedComm.forceAddSatedAct satedCommHelper (insertRollbackNode ()))

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
        val _ = addToAllThreads ()
        val comRbAid = insertCommitNode ()
        val _ = SatedComm.forceAddSatedAct satedCommHelper comRbAid
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
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlDecentralized.send(1)")
    val {actAid, waitNode} = handleSend {cid = c}
    val m = MLton.serialize (m)
    val _ = processLocalSend Client
              {channel = c, sendActAid = actAid,
               sendWaitNode = waitNode, value = m}
    val _ = S.atomicEnd ()
    val _ = if inNonSpecExecMode () then
              SatedComm.waitTillSated satedCommHelper (getLastAid ())
            else ()
  in
    ()
  end


  fun recv (CHANNEL c) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlDecentralized.recv(1)")
    val {actAid, waitNode} = handleRecv {cid = c}
    val serM = processLocalRecv Client
                {channel = c, recvActAid = actAid,
                 recvWaitNode = waitNode}
    val result = MLton.deserialize serM
    val _ = if inNonSpecExecMode () then
              SatedComm.waitTillSated satedCommHelper (getLastAid ())
            else ()
  in
    result
  end

  val exitDaemon = fn () => exitDaemon := true

  fun spawn f =
    let
      val tid = S.newTid ()
      val tidInt = C.tidToInt tid
      val {spawnAid, spawnNode}= handleSpawn {childTid = ThreadId tidInt}
      val _ = S.doAtomic (fn () => SatedComm.addSatedAct satedCommHelper spawnAid spawnNode)
      fun prolog () =
        let
          val _ = S.atomicBegin ()
          val _ = addToAllThreads ()
          val beginAct = handleInit {parentAid = spawnAid}
          val _ = SatedComm.forceAddSatedAct satedCommHelper beginAct
        in
          S.atomicEnd ()
        end
      fun safeBody () = (removeFromAllThreads(f(prolog()))) handle e => (removeFromAllThreads ();
                                                                     case e of
                                                                          CML.Kill => ()
                                                                        | _ => raise e)
      val _ = ignore (C.spawnWithTid (safeBody, tid))
    in
      ()
    end

  fun commit () =
  let
    val aid = getLastAid ()
    val _ = SatedComm.waitTillSated satedCommHelper aid

    (* All previous actions were stated. start the commit protocol *)
    val _ = S.atomicBegin ()
    val _ = POHelper.requestCommit ()
    val _ = blockCurrentThread ()
    val comAid = insertCommitNode ()
    val _ = SatedComm.forceAddSatedAct satedCommHelper comAid
    val _ = saveCont ()
    val _ = Assert.assertNonAtomic' ("DmlDecentralized.commit")
  in
    ()
  end


end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
