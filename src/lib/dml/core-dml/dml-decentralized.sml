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
  val addAid    : 'a t -> string -> StableGraph.action_id -> 'a -> unit
  val removeAid : 'a t -> string -> StableGraph.action_id -> unit
  val deque     : 'a t -> string -> (StableGraph.action_id * 'a) option
end

signature MATCHED_COMM =
sig
  type 'a t
  datatype 'a join_result =
    SUCCESS of 'a
  | FAILURE of {actAid : StableGraph.action_id,
                waitNode : StableGraph.node,
                value : 'a}
  | NOOP

  val empty   : unit -> 'a t
  val add     : 'a t -> {actAid: StableGraph.action_id,
                         remoteMatchAid: StableGraph.action_id,
                         waitNode: StableGraph.node} -> 'a -> unit
  val processJoin : 'a t -> {remoteAid: StableGraph.action_id,
                             withAid: StableGraph.action_id} -> 'a join_result
end

structure DmlDecentralized : DML =
struct
  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)

  open StableGraph

  structure IntDict = IntSplayDict
  structure StrDict = StringSplayDict
  structure S = CML.Scheduler
  structure C = CML
  structure ISS = IntSplaySet

  open RepTypes

  structure ZMQ = MLton.ZMQ
  (* -------------------------------------------------------------------- *)
  (* Datatype definitions *)
  (* -------------------------------------------------------------------- *)


  datatype msg = S_ACT  of {channel: channel_id, sendActAid: action_id, value: w8vec}
               | R_ACT  of {channel: channel_id, recvActAid: action_id}
               | S_JOIN of {channel: channel_id, sendActAid: action_id, recvActAid: action_id}
               | R_JOIN of {channel: channel_id, recvActAid: action_id, sendActAid: action_id}
               | J_REQ  of {pid: process_id}
               | J_ACK  of {pid: process_id}


  datatype 'a chan = CHANNEL of channel_id


  structure PendingComm : PENDING_COMM =
  struct
    open StrDict

    type 'a t = 'a AISD.dict StrDict.dict ref

    fun empty () = ref (StrDict.empty)

    fun addAid strDictRef channel aid value =
    let
      fun merge oldAidDict = AISD.insert oldAidDict aid value
    in
      strDictRef := StrDict.insertMerge (!strDictRef) channel (AISD.singleton aid value) merge
    end

    fun removeAid strDictRef channel aid =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      val aidDict = AISD.remove aidDict aid
    in
      strDictRef := StrDict.insert (!strDictRef) channel aidDict
    end handle StrDict.Absent => ()

    exception FIRST of action_id

    fun deque strDictRef channel =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      fun getOne () =
      let
        val _ = AISD.app (fn (k, _) => raise FIRST k) aidDict
      in
        raise AISD.Absent
      end handle FIRST k => k
      val aid = getOne ()
      val return = SOME (aid, AISD.lookup aidDict aid)
      val _ = removeAid strDictRef channel aid
    in
      return
    end handle AISD.Absent => NONE
  end

  structure MatchedComm : MATCHED_COMM =
  struct
    type 'a t = {actAid : action_id, waitNode : node, value : 'a} AISD.dict ref

    datatype 'a join_result =
      SUCCESS of 'a
    | FAILURE of {actAid : StableGraph.action_id,
                  waitNode : StableGraph.node,
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

    fun processJoin aidDictRef {remoteAid, withAid} =
    let
      val _ = if isAidLocal remoteAid then raise Fail "MatchedComm.processJoin"
              else ()
    in
      if not (isAidLocal withAid) then NOOP
      else
        let
          val {actAid, waitNode, value} = AISD.lookup (!aidDictRef) remoteAid
          val result =
            if MLton.equal (actAid, withAid) then SUCCESS value
            else FAILURE {actAid = actAid, waitNode = waitNode, value = value}
          val _ = aidDictRef := AISD.remove (!aidDictRef) remoteAid
        in
          result
        end
    end handle AISD.Absent => NOOP

  end

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})

  val pendingLocalSends : {sendWaitNode : node, value : w8vec} PendingComm.t = PendingComm.empty ()
  val pendingLocalRecvs : {recvWaitNode : node} PendingComm.t = PendingComm.empty ()
  val pendingRemoteSends : w8vec PendingComm.t = PendingComm.empty ()
  val pendingRemoteRecvs : unit PendingComm.t = PendingComm.empty ()

  val matchedSends : w8vec MatchedComm.t = MatchedComm.empty ()
  val matchedRecvs : w8vec MatchedComm.t = MatchedComm.empty ()

  val blockedThreads = ref (IntDict.empty)
  val allThreads = ref (IntDict.empty)

  (* State for join and exit*)
  val numPeers = ref ~1
  val peers = ref (ISS.empty)
  val exitDaemon = ref false

  (* -------------------------------------------------------------------- *)
  (* AllThreads Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun addToAllThreads () =
  let
    val newAllThreads = IntDict.insert (!allThreads) (S.tidInt ()) (S.getCurThreadId ())
  in
    allThreads := newAllThreads
  end

  fun removeFromAllThreads () =
  let
    val newAllThreads = IntDict.remove (!allThreads) (S.tidInt ())
  in
    allThreads := newAllThreads
  end

  fun tid2tid (ThreadId tid) = IntDict.find (!allThreads) tid

  (* -------------------------------------------------------------------- *)
  (* Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)
  fun debug'' fmsg = debug (fmsg)

  fun msgToString msg =
    case msg of
         S_ACT  {channel = ChannelId cidStr, sendActAid, ...} => concat ["S_ACT[", cidStr, ",", aidToString sendActAid, "]"]
       | R_ACT  {channel = ChannelId cidStr, recvActAid} => concat ["R_ACT[", cidStr, ",", aidToString recvActAid, "]"]
       | S_JOIN {channel = ChannelId cidStr, sendActAid, recvActAid} => concat ["S_JOIN[", cidStr, ",", aidToString sendActAid, ",", aidToString recvActAid, "]"]
       | R_JOIN {channel = ChannelId cidStr, recvActAid, sendActAid} => concat ["R_JOIN[", cidStr, ",", aidToString sendActAid, ",", aidToString recvActAid, "]"]
       | J_REQ  {pid = ProcessId pidInt} => concat ["J_REQ[", Int.toString pidInt, "]"]
       | J_ACK  {pid = ProcessId pidInt} => concat ["J_ACK[", Int.toString pidInt, "]"]

  val emptyW8Vec = Vector.tabulate (0, fn _ => 0wx0)

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
    val t = IntDict.lookup (!blockedThreads) tidInt handle IntDict.Absent => raise Fail "DmlDecentralized.unblockThread: Absent"
    val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
    val rt = S.prepVal (t, value)
  in
    S.ready rt
  end

  (* -------------------------------------------------------------------- *)
  (* Message Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun msgSend (msg : msg) =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.msgSend", SOME 1)
    val PROXY {sink, ...} = !proxy
    val _ = ZMQ.send (valOf sink, msg)
  in
    ()
  end

  fun msgSendSafe msg =
  let
    val _ = Assert.assertNonAtomic' ("DmlDecentralized.msgSendSafe")
    val _ = S.atomicBegin ()
    val _ = msgSend msg
    val _ = S.atomicEnd ()
  in
    ()
  end

  fun msgRecv () : msg option =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.msgRecv", SOME 1)
    val PROXY {source, ...} = !proxy
  in
    ZMQ.recvNB (valOf source)
  end

  fun msgRecvSafe () =
  let
    val _ = Assert.assertNonAtomic' ("DmlDecentralized.msgRecvSafe")
    val _ = S.atomicBegin ()
    val r = msgRecv ()
    val _ = S.atomicEnd ()
  in
    r
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

  datatype caller_kind = Client | Daemon

  fun processLocalSend callerKind {channel = c, sendActAid, sendWaitNode, value} =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalSend(1)", SOME 1)
    val _ =
      case PendingComm.deque pendingLocalRecvs c of
          NONE => (* No matching receives, check remote *)
            (case PendingComm.deque pendingRemoteRecvs c of
                  NONE => (* No matching remote recv either *)
                    let
                      val _ = PendingComm.addAid pendingLocalSends c sendActAid
                        {sendWaitNode = sendWaitNode, value = value}
                      val _ = case callerKind of
                                  Client => blockCurrentThread ()
                                | Daemon => emptyW8Vec
                    in
                      ()
                    end
                | SOME (recvActAid, ()) => (* matching remote recv *)
                    let
                      val _ = setMatchAid sendWaitNode recvActAid
                      val _ = msgSend (S_JOIN {channel = ChannelId c, sendActAid = sendActAid, recvActAid = recvActAid})
                      val _ = MatchedComm.add matchedSends
                        {actAid = sendActAid, remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
                      val _ = case callerKind of
                                  Client => blockCurrentThread ()
                                | Daemon => emptyW8Vec
                    in
                      ()
                    end)
        | SOME (recvActAid, {recvWaitNode}) => (* matching local recv *)
            let
              val _ = setMatchAid sendWaitNode recvActAid
              val _ = setMatchAid recvWaitNode sendActAid
              val _ = msgSend (S_JOIN {channel = ChannelId c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = ChannelId c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = resumeThread (aidToTidInt recvActAid) value
              val _ = case callerKind of
                          Daemon => resumeThread (aidToTidInt sendActAid) emptyW8Vec
                        | Client => S.atomicEnd ()
            in
              ()
            end
  in
    case callerKind of
         Client => Assert.assertNonAtomic' ("DmlDecentralized.processLocalSend(2)")
       | Daemon => Assert.assertAtomic' ("DmlDecentralized.processLocalSend(2)", SOME 1)
  end

  fun processLocalRecv callerKind {channel = c, recvActAid, recvWaitNode} =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.processLocalRecv(1)", SOME 1)
    val value =
      case PendingComm.deque pendingLocalSends c of
          NONE => (* No local matching sends, check remote *)
            (case PendingComm.deque pendingRemoteSends c of
                  NONE => (* No matching remote send either *)
                    let
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
                      val _ = setMatchAid recvWaitNode sendActAid
                      val _ = msgSend (R_JOIN {channel = ChannelId c, sendActAid = sendActAid, recvActAid = recvActAid})
                      val _ = MatchedComm.add matchedRecvs
                        {actAid = recvActAid, remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
                      val value = case callerKind of
                                  Client => blockCurrentThread ()
                                | Daemon => value
                    in
                      value
                    end)
        | SOME (sendActAid, {sendWaitNode, value}) => (* matching local send *)
            let
              val _ = setMatchAid sendWaitNode recvActAid
              val _ = setMatchAid recvWaitNode sendActAid
              val _ = msgSend (S_JOIN {channel = ChannelId c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = msgSend (R_JOIN {channel = ChannelId c, sendActAid = sendActAid, recvActAid = recvActAid})
              val _ = resumeThread (aidToTidInt sendActAid) emptyW8Vec
              val () = case callerKind of
                          Daemon => resumeThread (aidToTidInt recvActAid) value
                        | Client => S.atomicEnd ()
            in
              value
            end
    val _ = case callerKind of
                 Client => Assert.assertNonAtomic' ("DmlDecentralized.processLocalRecv(2)")
               | Daemon => Assert.assertAtomic' ("DmlDecentralized.processLocalRecv(2)", SOME 1)
  in
    value
  end

  fun processMsg msg =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.processMsg", SOME 1)
  in
    case msg of
        S_ACT {channel as ChannelId c, sendActAid, value} =>
        let
          val _ = Assert.assert ([], fn () => "DmlDecentralized.processMsg: remote S_ACT",
                                 fn () => not (isAidLocal sendActAid))
        in
          (case PendingComm.deque pendingLocalRecvs c of
              NONE => (* No matching receives *)
                PendingComm.addAid pendingRemoteSends c sendActAid value
            | SOME (recvActAid, {recvWaitNode}) => (* matching recv *)
                let
                  val _ = setMatchAid recvWaitNode sendActAid
                  val _ = msgSend (R_JOIN {channel = channel, sendActAid = sendActAid, recvActAid = recvActAid})
                in
                  MatchedComm.add matchedRecvs
                    {actAid = recvActAid, remoteMatchAid = sendActAid, waitNode = recvWaitNode} value
                end)
        end
      | R_ACT {channel as ChannelId c, recvActAid} =>
        let
          val _ = Assert.assert ([], fn () => "DmlDecentralized.processMsg: remote R_ACT",
                                 fn () => not (isAidLocal recvActAid))
        in
          (case PendingComm.deque pendingLocalSends c of
              NONE => (* No matching sends *)
                PendingComm.addAid pendingRemoteRecvs c recvActAid ()
            | SOME (sendActAid, {sendWaitNode, value}) => (* matching send *)
               let
                 val _ = setMatchAid sendWaitNode recvActAid
                 val _ = msgSend (S_JOIN {channel = channel, sendActAid = sendActAid, recvActAid = recvActAid})
               in
                MatchedComm.add matchedSends
                  {actAid = sendActAid, remoteMatchAid = recvActAid, waitNode = sendWaitNode} value
               end)
        end
      | S_JOIN {channel = ChannelId c, sendActAid, recvActAid} =>
          (case MatchedComm.processJoin matchedRecvs {remoteAid = sendActAid, withAid = recvActAid} of
               MatchedComm.NOOP => ()
             | MatchedComm.SUCCESS value => resumeThread (aidToTidInt recvActAid) value
             | MatchedComm.FAILURE {actAid = recvActAid, waitNode = recvWaitNode, ...} =>
                 ignore (processLocalRecv Daemon {channel = c, recvActAid = recvActAid, recvWaitNode = recvWaitNode}))
      | R_JOIN {channel = ChannelId c, recvActAid, sendActAid} =>
          (case MatchedComm.processJoin matchedSends {remoteAid = recvActAid, withAid = sendActAid} of
               MatchedComm.NOOP => ()
             | MatchedComm.SUCCESS _ => resumeThread (aidToTidInt sendActAid) emptyW8Vec
             | MatchedComm.FAILURE {actAid = sendActAid, waitNode = sendWaitNode, value} =>
                 ignore (processLocalSend Daemon {channel = c, sendActAid = sendActAid, sendWaitNode = sendWaitNode, value = value}))
      | _ => ()
  end

  fun clientDaemon () =
    if (!exitDaemon) then ()
    else
      case msgRecvSafe () of
          NONE => (C.yield (); clientDaemon ())
        | SOME m =>
            let
              val _ = debug'' (fn () => msgToString m)
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
      val n = if n = 10000 then
                (msgSendSafe (J_REQ {pid = ProcessId (!processId)}); 0)
              else n+1
      val () = case msgRecvSafe () of
                    NONE => join n
                  | SOME (J_ACK {pid = ProcessId pidInt}) =>
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
        (* start the daemon. Insert a CR_RB node just so that functions which
         * expect a node at every tid will not throw exceptions. *)
        val _ = C.spawn (fn () =>
                  let
                    val _ = insertCommitRollbackNode ()
                  in
                    clientDaemon ()
                  end)
        val _ = addToAllThreads ()
        val _ = insertCommitRollbackNode ()
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
    val ChannelId cstr = c
  in
    processLocalSend Client
      {channel = cstr, sendActAid = actAid,
       sendWaitNode = waitNode, value = m}
  end


  fun recv (CHANNEL c) =
  let
    val _ = debug' ("DmlDecentralized.recv(1)")
    val {actAid, waitNode} = handleRecv {cid = c}
    val ChannelId cstr = c
    val serM = processLocalRecv Client
                {channel = cstr, recvActAid = actAid,
                 recvWaitNode = waitNode}
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
