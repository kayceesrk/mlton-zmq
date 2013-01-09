(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure DmlNonSpeculative : DML =
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

end

structure DmlCentralized : DML =
struct
  structure ZMQ = MLton.ZMQ

  type thread_id  = ThreadId of int
  type node_id    = NodeId of int
  type channel_id = ChannelId of string

  datatype proxy = PROXY of {context : ZMQ.context,
                             sink: ZMQ.socket,
                             source: ZMQ.socket}

  datatype 'a chan = Channel of {cid: channel_id, pxy: proxy}

  datatype 'a content = S_REQ of 'a
                      | R_REQ
                      | S_ACK of thread_id
                      | R_ACK of (thread_id * 'a)

  datatype 'a msg = {cid : channel_id,
                     nid : node_id,
                     tid : thread_id,
                     cnt : 'a content}


  fun startProxy {frontend = fe_str, backend = be_str} =
  let
    val context = ZMQ.ctxNew ()
    val frontend = ZMQ.sockCreate (context, ZMQ.Sub)
    val backend = ZMQ.sockCreate (context, ZMQ.Pub)
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

  fun channel (p,s) = Channel {cid = ChannelId s, pxy = p}

  fun send (c, m) =


end
