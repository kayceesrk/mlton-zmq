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

  type ptr = {pid: process_id, tid: thread_id, rid: int}

  datatype action_type = SEND_WAIT of {cid: channel_id, matchAid: action_id option}
                       | SEND_ACT of {cid: channel_id}
                       | RECV_WAIT of {cid: channel_id, matchAid: action_id option}
                       | RECV_ACT of {cid: channel_id}
                       | SPAWN of {childTid: thread_id}
                       | BEGIN of {parentAid: action_id}
                       | COM_RB (* This indicates the node that is inserted after commit or rollback *)

  datatype action = ACTION of {aid: action_id, act: action_type}

  datatype msg = S_ACT  of {channel: channel_id, sendActAid: action_id, value: w8vec}
               | R_ACT  of {channel: channel_id, recvActAid: action_id}
               | S_JOIN of {channel: channel_id, sendActAid: action_id, recvActAid: action_id}
               | R_JOIN of {channel: channel_id, recvActAid: action_id, sendActAid: action_id}
               | CONN   of {pid: process_id}
               | SATED  of {recipient: process_id, remoteAid: action_id, matchAid: action_id}
               (* Arbitrator Communication *)
               | AR_REQ_ADD of {action: action, prevAction: action option}
               | AR_REQ_COM of {action: action}

  datatype rooted_msg = ROOTED_MSG of {sender: process_id, msg: msg}

  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})
  val processId = ref ~1
end
