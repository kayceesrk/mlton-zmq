(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

(* structure DmlNonSpeculative : DML =
struct
  structure ZMQ = MLton.ZMQ

  datatype proxy = PROXY of {context : ZMQ.context,
                         sink: ZMQ.socket,
                         source: ZMQ.socket}

  datatype 'a chan = CHAN

  fun startProxy {frontend = fe_str, backend = be_str} =
  let
    val context = ZMQ.ctxNew ()
    val frontend = ZMQ.sockCreate (context, ZMQ.XSub)
    val backend = ZMQ.sockCreate (context, ZMQ.XPub)
    val _ = ZMQ.sockBind (frontend, fe_str)
    val _ = ZMQ.sockBind (backend, be_str)
  in
    ZMQ.proxy {frontend = frontend, backend = backend}
  end

  fun connect {sink = sink_str, source = source_str} =
  let
    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
  in
    PROXY {context = context, source = source, sink = sink}
  end

  fun channel _ = raise Fail "DmlNonSpeculative.channel: Not Implemented!"
  fun send    _ = raise Fail "DmlNonSpeculative.send: Not Implemented!"
  fun recv    _ = raise Fail "DmlNonSpeculative.recv: Not Implemented!"

end *)

structure DmlCentralized : DML =
struct
  structure Assert = LocalAssert(val assert = false)
  structure ZMQ = MLton.ZMQ
  structure R = IntRedBlackDict
  structure S = CML.Scheduler


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

  datatype chan = CHANNEL of channel_id

  datatype content = S_REQ of w8vec
                   | R_REQ
                   | S_ACK of thread_id
                   | R_ACK of (thread_id * w8vec)
                   | J_REQ
                   | J_ACK


  datatype msg = MSG of {cid : channel_id,
                         nid : node_id,
                         tid : thread_id,
                         cnt : content}

  (* -------------------------------------------------------------------- *)
  (* state *)
  (* -------------------------------------------------------------------- *)

  val blockedThreads = ref (R.empty)
  val nodeId = ref ~1
  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})

  (* -------------------------------------------------------------------- *)
  (* Helper Functions *)
  (* -------------------------------------------------------------------- *)

  fun contentToStr cnt =
    case cnt of
         S_REQ _ => "S_REQ"
       | R_REQ => "R_REQ"
       | S_ACK (ThreadId i) => ("S_ACK(" ^ (Int.toString i) ^ ")")
       | R_ACK (ThreadId i, _) => ("R_ACK(" ^ (Int.toString i) ^ ")")
       | J_REQ => "J_REQ"
       | J_ACK => "J_ACK"

  fun msgToString (MSG {cid = ChannelId cstr,
                   tid = ThreadId tint,
                   nid = NodeId nint,
                   cnt}) =
    concat ["MSG -- Channel: ", cstr, " Thread: ", Int.toString tint,
            " Node: ", Int.toString nint, contentToStr cnt]


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

    (* main loop *)
    fun processLoop () =
    let
      val m : msg = case ZMQ.recvNB frontend of
                            NONE => processLoop ()
                          | SOME m => m
      val _ = print ((msgToString m) ^ "\n")
    in
      processLoop ()
    end

    val _ = processLoop ()
  in
    ()
  end

  (* -------------------------------------------------------------------- *)
  (* Clients *)
  (* -------------------------------------------------------------------- *)

  fun connect {sink = sink_str, source = source_str, nodeId = nid} =
  let
    val context = ZMQ.ctxNew ()
    val source = ZMQ.sockCreate (context, ZMQ.Sub)
    val sink = ZMQ.sockCreate (context, ZMQ.Pub)
    val _ = ZMQ.sockConnect (source, source_str)
    val _ = ZMQ.sockConnect (sink, sink_str)
    val _ = nodeId := nid

    fun join () =
    let
      val _ = ZMQ.send (sink, MSG {cid = ChannelId "bogus", nid = NodeId (!nodeId),
                        tid = ThreadId ~1, cnt = J_REQ})
      val m : msg = case ZMQ.recvNB source of
                            NONE => join ()
                          | SOME m => m
    in
      m
    end

    val _ = join ()

    (* If we get here, then it means that we have joined *)

    val _ = proxy := PROXY {context = SOME context,
                            source = SOME source,
                            sink = SOME sink}
  in
    ()
  end

  fun runDML (body, to) =
    let
      val _ = Assert.assert ([], fn () => "runDML must be run after connect",
                             fn () => case !proxy of
                                        PROXY {sink = NONE, ...} => false
                                      | _ => true)
    in
      RunCML.doit (body, to)
    end

  fun channel s = CHANNEL (ChannelId s)

  fun send (CHANNEL c, m) =
  let
    val _ = S.switchToNext (fn t : w8vec S.thread =>
              let
                val tid = S.tidInt ()
                val PROXY {sink, ...} = !proxy
                val _ = ZMQ.send (valOf sink, MSG {cid = c, nid = NodeId (!nodeId),
                                  tid = ThreadId tid, cnt = S_REQ m})
                val _ = blockedThreads := (R.insert (!blockedThreads) tid t)
              in
                ()
              end)
  in
    ()
  end

  fun recv (CHANNEL c) =
    S.switchToNext (fn t : w8vec S.thread =>
      let
        val tid = S.tidInt ()
        val PROXY {sink, ...} = !proxy
        val _ = ZMQ.send (valOf sink, {cid = c, nid = NodeId (!nodeId),
                          tid = ThreadId tid, cnt = R_REQ})
        val _ = blockedThreads := (R.insert (!blockedThreads) tid t)
      in
        ()
      end)
  (* -------------------------------------------------------------------- *)
end

structure Dml = DmlCentralized

(* TODO -- filters *)
