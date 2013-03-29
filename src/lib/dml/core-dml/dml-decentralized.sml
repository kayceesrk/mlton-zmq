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
  val add   : 'a t -> {channel: RepTypes.channel_id,
                       actAid: ActionManager.action_id,
                       remoteMatchAid: ActionManager.action_id,
                       waitNode: POHelper.node} -> 'a -> unit
  val join  : 'a t -> {remoteAid: ActionManager.action_id,
                       withAid: ActionManager.action_id} -> 'a join_result
  val cleanup : 'a t -> int RepTypes.PTRDict.dict ->
                {channel: RepTypes.channel_id, actAid: ActionManager.action_id,
                 waitNode: POHelper.node, value: 'a} list
end

signature IVAR =
sig
  val new : unit -> {read: unit -> 'a, write: 'a -> unit}
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

    type 'a t = 'a AidDict.dict StrDict.dict ref

    fun empty () = ref (StrDict.empty)

    fun addAid strDictRef (ChannelId channel) aid value =
    let
      fun merge oldAidDict = AidDict.insert oldAidDict aid value
    in
      strDictRef := StrDict.insertMerge (!strDictRef) channel (AidDict.singleton aid value) merge
    end

    fun removeAid strDictRef (ChannelId channel) aid =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      val aidDict = AidDict.remove aidDict aid
    in
      strDictRef := StrDict.insert (!strDictRef) channel aidDict
    end handle StrDict.Absent => ()

    exception FIRST of action_id

    fun deque strDictRef (ChannelId channel) {againstAid} =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      fun getOne () =
      let
        val _ = AidDict.app (fn (k, _) =>
                  if (aidToTidInt k = aidToTidInt againstAid) andalso
                     (aidToPidInt k = aidToPidInt againstAid)
                  then () (* dont match actions from the same thread *)
                  else raise FIRST k) aidDict
      in
        raise AidDict.Absent
      end handle FIRST k => k
      val aid = getOne ()
      val return = SOME (aid, AidDict.lookup aidDict aid)
      val _ = removeAid strDictRef (ChannelId channel) aid
    in
      return
    end handle AidDict.Absent => NONE
             | StrDict.Absent => NONE

    fun cleanup strDictRef rollbackAids =
    let
      val _ = debug (fn () => "PendingComm.cleanup: length="^(Int.toString (StrDict.size(!strDictRef))))
      val _ = Assert.assertAtomic' ("PendingComm.cleanup", SOME 1)
      val oldStrDict = !strDictRef
      fun getNewAidDict oldAidDict =
        let
          val emptyAidDict = AidDict.empty
        in
          AidDict.foldl (fn (aid as ACTION_ID {pid, tid, rid, ...}, value, newAidDict) =>
            case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
                NONE => AidDict.insert newAidDict aid value
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
    type 'a t = {channel : channel_id, actAid : action_id,
                 waitNode : node, value : 'a} AidDict.dict ref

    datatype 'a join_result =
      SUCCESS of {value : 'a, waitNode: POHelper.node}
    | FAILURE of {actAid : ActionManager.action_id,
                  waitNode : POHelper.node,
                  value : 'a}
    | NOOP

    fun empty () = ref (AidDict.empty)

    fun add aidDictRef {channel, actAid, remoteMatchAid, waitNode} value =
    let
      val _ = if isAidLocal remoteMatchAid then raise Fail "MatchedComm.add(1)"
              else if not (isAidLocal actAid) then raise Fail "MatchedComm.add(2)"
              else ()
    in
      aidDictRef := AidDict.insert (!aidDictRef) remoteMatchAid {channel = channel,
                      actAid = actAid, waitNode = waitNode, value = value}
    end

    fun join aidDictRef {remoteAid, withAid} =
    let
      val _ = if isAidLocal remoteAid then raise Fail "MatchedComm.join"
              else ()
      val {actAid, waitNode, value, channel = _} = AidDict.lookup (!aidDictRef) remoteAid
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
      val _ = aidDictRef := AidDict.remove (!aidDictRef) remoteAid
    in
      result
    end handle AidDict.Absent => (debug' ("NOOP"); NOOP)

    fun cleanup aidDictRef rollbackAids =
    let
      val oldAidDict = !aidDictRef
      val (newAidDict, failList) =
        AidDict.foldl (fn (aid as ACTION_ID {pid, tid, rid, ...} (* key *),
                          failValue as {actAid, waitNode = _ , value = _, channel = _} (* value *),
                          (newAidDict, failList) (* acc *)) =>
          case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
               NONE => (AidDict.insert newAidDict aid failValue, failList)
             | SOME _ =>
                 let (* Make sure the blocked (local) thread, is not part of the rollback set *)
                   val ACTION_ID {pid, tid, rid, ...} = actAid
                 in
                   case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
                        NONE => (AidDict.insert newAidDict aid failValue, failList)
                      | SOME _ => (newAidDict, failValue::failList)
                 end) (AidDict.empty, []) oldAidDict
      val _ = aidDictRef := newAidDict
    in
      failList
    end


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

  (* State for join and exit*)
  val peers = ref (ISS.empty)
  val exitDaemon = ref false

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
    fun rollbackBlockedThread t =
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
    fun restoreSCore (rthrd as S.RTHRD (cmlTid, _)) =
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
  (* Simle IVar *)
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
        val _ = Assert.assertNonAtomic' ("IVar.write")
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
                   | THREAD _ => raise Fail "IVar.read: some thread waiting!"
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
  (* Message filter *)
  (* -------------------------------------------------------------------- *)

  structure MessageFilter =
  struct
    structure Assert = LocalAssert(val assert = true)
    structure Debug = LocalDebug(val debug = false)

    fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)

    structure PTOrdered :> ORDERED
      where type t = {pid: process_id, tid: thread_id} =
      struct
        type t = {pid: process_id, tid: thread_id}

        val eq = MLton.equal
        val _ = eq

        fun compare ({pid = ProcessId pidInt1, tid = ThreadId tidInt1},
                     {pid = ProcessId pidInt2, tid = ThreadId tidInt2}) =
          case Int.compare (pidInt1, pidInt2) of
               EQUAL => Int.compare (tidInt1, tidInt2)
             | lg => lg
      end

    structure PTDict = SplayDict (structure Key = PTOrdered)

    val filterRef = ref PTDict.empty

    fun addToFilter rollbackAids =
      let
        val _ = Assert.assertAtomic' ("MessageFilter.addFilter", NONE)
        val newFilter = ListMLton.fold (PTRDict.domain rollbackAids, PTDict.empty,
          fn ({pid, tid, rid}, newFilter) => PTDict.insert newFilter {pid = pid, tid = tid} rid)
        val oldFilter = !filterRef
        val newFilter = PTDict.union oldFilter newFilter (fn (_,i,j) => if i>j then i else j)
      in
        filterRef := newFilter
      end

    fun isAllowed (aid as ACTION_ID {pid, tid, rid, ...}) =
      case PTDict.find (!filterRef) {pid = pid, tid = tid} of
           NONE =>
           let
             val _ = debug (fn () => "MessageFilter: blocking aid="^(aidToString aid))
           in
             true
           end
         | SOME rid' =>
             if rid <= rid' then false
             else (* if rid > rid', then we remove the entry from filter *)
               let
                 val ProcessId pidInt = pid
                 val ThreadId tidInt = tid
                 val _ = debug (fn () => "MessageFilter: removing filter (pid="^(Int.toString pidInt)
                                        ^",tid="^(Int.toString tidInt)^")")
                 val _ = filterRef := PTDict.remove (!filterRef) {pid = pid, tid = tid}
               in
                 true
               end
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
                        val _ = MatchedComm.add matchedSends {channel = c, actAid = sendActAid,
                                  remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
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
              (* Resume blocked recv *)
              val tidInt = aidToTidInt recvActAid
              val _ = resumeThread tidInt value
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
                        val _ = MatchedComm.add matchedRecvs {channel = c, actAid = recvActAid,
                                  remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
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
              val () = case callerKind of
                          Daemon => resumeThread (aidToTidInt recvActAid) value
                        | Client => S.atomicEnd ()
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

  fun processRollbackMsg rollbackAids dfsStartAct =
    let
      (* Cleanup dependence graph *)
      val _ = CML.atomicSpawn (fn () => markCycleDepGraph dfsStartAct)
      (* Clean up pending acts *)
      val () = PendingComm.cleanup pendingLocalSends rollbackAids
      val () = PendingComm.cleanup pendingRemoteSends rollbackAids
      val () = PendingComm.cleanup pendingLocalRecvs rollbackAids
      val () = PendingComm.cleanup pendingRemoteRecvs rollbackAids
      (* Cleanup matched acts *)
      val failList = MatchedComm.cleanup matchedSends rollbackAids
      val _ = ListMLton.map (failList, fn {channel, actAid, waitNode, value} =>
                processLocalSend Daemon {channel = channel, sendActAid = actAid,
                sendWaitNode = waitNode, value = value})
      val failList = MatchedComm.cleanup matchedRecvs rollbackAids
      val _ = ListMLton.map (failList, fn {channel, actAid, waitNode, value = _} =>
                processLocalRecv Daemon {channel = channel, recvActAid = actAid, recvWaitNode = waitNode})
      (* Add message filter *)
      val _ = MessageFilter.addToFilter rollbackAids
      (* rollback threads *)
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
          if MessageFilter.isAllowed sendActAid then
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
          if MessageFilter.isAllowed recvActAid then
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
      | S_JOIN {channel = c, sendActAid, recvActAid} =>
          if MessageFilter.isAllowed sendActAid then
            let
              val _ = PendingComm.removeAid pendingRemoteSends c sendActAid
              val _ = PendingComm.removeAid pendingRemoteRecvs c recvActAid
            in
              case MatchedComm.join matchedRecvs {remoteAid = sendActAid, withAid = recvActAid} of
                  MatchedComm.NOOP => ()
                | MatchedComm.SUCCESS {value, waitNode = recvWaitNode} =>
                    let
                      val _ = setMatchAid recvWaitNode sendActAid
                      (* XXX KC -- TODO start *)
                      val tidInt = aidToTidInt recvActAid
                      val _ = resumeThread tidInt value
                    in
                      ()
                    end
                | MatchedComm.FAILURE {actAid = recvActAid, waitNode = recvWaitNode, ...} =>
                      ignore (processLocalRecv Daemon {channel = c, recvActAid = recvActAid, recvWaitNode = recvWaitNode})
                      (* XXX KC -- TODO end *)
            end
          else ()
      | R_JOIN {channel = c, recvActAid, sendActAid} =>
          if MessageFilter.isAllowed recvActAid then
            let
              val _ = PendingComm.removeAid pendingRemoteSends c sendActAid
              val _ = PendingComm.removeAid pendingRemoteRecvs c recvActAid
            in
              case MatchedComm.join matchedSends {remoteAid = recvActAid, withAid = sendActAid} of
                MatchedComm.NOOP => ()
              | MatchedComm.SUCCESS {waitNode = sendWaitNode, ...} =>
                  let
                    val _ = setMatchAid sendWaitNode recvActAid
                  in
                    ()
                  end
              | MatchedComm.FAILURE {actAid = sendActAid, waitNode = sendWaitNode, value} =>
                    ignore (processLocalSend Daemon {channel = c, sendActAid = sendActAid, sendWaitNode = sendWaitNode, value = value})
            end
          else ()
      | AR_RES_SUCC {dfsStartAct = _} => ()
          (* If you have the committed thread in your finalSatedComm structure, move to memoized *)
      | AR_RES_FAIL {dfsStartAct, rollbackAids} => processRollbackMsg rollbackAids dfsStartAct
      | AR_REQ_ADD {action as ACTION{aid, ...}, prevAction} =>
          if MessageFilter.isAllowed aid then
            processAdd {action = action, prevAction = prevAction}
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
                        if ISS.size (!peers) = (numPeers-1) then msgSendSafe (CONN {pid = ProcessId (!processId)})
                        else join n
                      end
                  | SOME m => raise Fail ("DmlDecentralized.connect: unexpected message during connect" ^ (msgToString m))
    in
      ()
    end
    val _ = if numPeers = 1 then () else join 0
    (* If we get here, then we have joined *)
    val _ = debug' ("DmlDecentralized.connect.join(3)")
  in
    ()
  end

  fun saveCont () = POHelper.saveCont (fn () => ignore(insertRollbackNode ()))

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

  fun send (CHANNEL c, m) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlDecentralized.send(1)")
    val {actAid, waitNode} = handleSend {cid = c}
    val m = MLton.serialize (m)
    val _ = processLocalSend Client
              {channel = c, sendActAid = actAid,
               sendWaitNode = waitNode, value = m}
    val _ = if inNonSpecExecMode () andalso not (POHelper.isLastNodeMatched ()) then
              let
                val {read = wait, write = wakeup} = IVar.new ()
                val _ = POHelper.doOnUpdateLastNode wakeup
                val _ = S.atomicEnd ()
              in
                wait ()
              end
            else S.atomicEnd ()
    val _ = Assert.assertNonAtomic' ("send")
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
  in
    result
  end

  val exitDaemon = fn () => exitDaemon := true


  fun commit () =
  let
    val finalAction = POHelper.getFinalAction ()
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
                     restoreCont ()
                   end
               | _ => raise Fail "DmlDecentralized.commit: unexpected message"

    (* Committed Successfully *)
    val _ = debug' ("DmlDecentralized.commit: SUCCESS")
    (* --- Racy code --- *)
    val _ = CML.tidCommit (S.getCurThreadId ())
    (* --- End racy code --- *)
    val _ = insertCommitNode ()
    val _ = saveCont ()
    val _ = Assert.assertNonAtomic' ("DmlDecentralized.commit")
  in
    ()
  end

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

end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
