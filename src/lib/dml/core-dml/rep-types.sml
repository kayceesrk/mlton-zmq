(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure RepTypes =
struct
  structure ZMQ = MLton.ZMQ

  type w8vec = Word8.word vector

  datatype thread_id  = ThreadId of int
  datatype process_id = ProcessId of int
  datatype channel_id = ChannelId of string


  datatype proxy = PROXY of {context : ZMQ.context option,
                             sink: ZMQ.socket option,
                             source: ZMQ.socket option}

  datatype action_id = ACTION_ID of {pid: process_id,
                                     tid: thread_id,
                                     rid: int,
                                     aid: int}

  datatype msg = S_ACT  of {channel: channel_id, sendActAid: action_id, value: w8vec}
               | R_ACT  of {channel: channel_id, recvActAid: action_id}
               | S_JOIN of {channel: channel_id, sendActAid: action_id, recvActAid: action_id}
               | R_JOIN of {channel: channel_id, recvActAid: action_id, sendActAid: action_id}
               | CONN   of {pid: process_id}
               | SATED  of {recipient: process_id, remoteAid: action_id, matchAid: action_id}

  datatype rooted_msg = ROOTED_MSG of {sender: process_id, msg: msg}

  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})
  val processId = ref ~1
end
