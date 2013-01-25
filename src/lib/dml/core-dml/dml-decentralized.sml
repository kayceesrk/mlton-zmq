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
    SUCCESS
  | FAILURE of (unit DirectedGraph.Node.t * 'a)
  | NOOP

  val empty   : unit -> 'a t
  val add     : 'a t -> {actAid: StableGraph.action_id,
                         remoteMatchAid: StableGraph.action_id,
                         waitNode: unit DirectedGraph.Node.t} -> 'a -> unit
  val processJoin : 'a t -> {remoteAid: StableGraph.action_id,
                             withAid: StableGraph.action_id} -> 'a join_result
end

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
    end handle Absent => ()

    exception FIRST of action_id

    fun deque strDictRef channel =
    let
      val aidDict = StrDict.lookup (!strDictRef) channel
      fun getOne () =
      let
        val _ = AISD.app (fn (k, _) => raise FIRST k) aidDict
      in
        raise Absent
      end handle FIRST k => k
      val aid = getOne ()
      val return = SOME (aid, AISD.lookup aidDict aid)
      val _ = removeAid strDictRef channel aid
    in
      return
    end handle Absent => NONE
  end

  structure MatchedComm : MATCHED_COMM =
  struct
    type 'a t = {actAid : action_id, waitNode : node, value : 'a} AISD.dict ref

    datatype 'a join_result =
      SUCCESS
    | FAILURE of (unit DirectedGraph.Node.t * 'a)
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
            if MLton.equal (actAid, withAid) then SUCCESS
            else FAILURE (waitNode, value)
          val _ = aidDictRef := AISD.remove (!aidDictRef) remoteAid
        in
          result
        end
    end handle Absent => NOOP

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
  val matchedRecvs : unit MatchedComm.t = MatchedComm.empty ()

  val blockedThreads = ref (IntDict.empty)
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
    val () = ()
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
    ()
  end


  fun recv (CHANNEL c) =
  let
    val _ = debug' ("DmlDecentralized.recv(1)")
    val {actAid, waitNode} = handleRecv {cid = c}
  in
    raise Fail "DmlDecentralized.recv: not implemented"
  end

  val exitDaemon = fn () => exitDaemon := true

  fun spawn f = ignore (C.spawn f)

end

(* TODO -- Messaegs will be dropped if HWM is reached!! *)
