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
                                     rid: int, (* revision id *)
                                     aid: int, (* action number *)
                                     vid: int} (* version *)

  structure ActionIdOrdered
    :> ORDERED where type t = action_id
  = struct
    type t = action_id

    fun eq (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, aid = aid1, ...},
            ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, aid = aid2, ...}) =
            MLton.equal ((pid1,tid1,rid1,aid1),(pid2,tid2,rid2,aid2))
    val _  = eq
    fun compare (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, aid = aid1, ...},
                 ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, aid = aid2, ...}) =
      (case Int.compare (pid1, pid2) of
           EQUAL => (case Int.compare (tid1, tid2) of
                          EQUAL => (case Int.compare (rid1, rid2) of
                                         EQUAL => Int.compare (aid1, aid2)
                                       | lg => lg)
                        | lg => lg)
         | lg => lg)
  end

  structure AidDict = SplayDict (structure Key = ActionIdOrdered)
  structure AidSet = SplaySet (structure Elem = ActionIdOrdered)

  type ptr = {pid: process_id, tid: thread_id, rid: int}

  structure PTROrdered :> ORDERED where type t = ptr =
  struct
    type t = ptr

    val eq = MLton.equal
    val _ = eq
    fun compare ({pid = ProcessId pidInt1, tid = ThreadId tidInt1, rid = rid1},
                 {pid = ProcessId pidInt2, tid = ThreadId tidInt2, rid = rid2}) =
      (case Int.compare (pidInt1, pidInt2) of
            EQUAL => (case Int.compare (tidInt1, tidInt2) of
                           EQUAL => Int.compare (rid1, rid2)
                         | lg => lg)
          | lg => lg)

  end

  structure PTRDict = SplayDict (structure Key = PTROrdered)

  datatype action_type = SEND_WAIT of {cid: channel_id, matchAid: action_id option}
                       | SEND_ACT of {cid: channel_id}
                       | RECV_WAIT of {cid: channel_id, matchAid: action_id option}
                       | RECV_ACT of {cid: channel_id}
                       | SPAWN of {childTid: thread_id}
                       | BEGIN of {parentAid: action_id}
                       | COM (* This indicates the node that is inserted after commit or rollback *)
                       | RB

  datatype action = ACTION of {aid: action_id, act: action_type}

  datatype msg = S_ACT  of {channel: channel_id, sendActAid: action_id, value: w8vec}
               | R_ACT  of {channel: channel_id, recvActAid: action_id}
               | S_JOIN of {channel: channel_id, sendActAid: action_id, recvActAid: action_id}
               | R_JOIN of {channel: channel_id, recvActAid: action_id, sendActAid: action_id}
               | CONN   of {pid: process_id}
               (* CycleDetector Communication *)
               | AR_REQ_ADD of {action: action, prevAction: action option}
               | AR_RES_SUCC of {dfsStartAct: action}
               | AR_RES_FAIL of {rollbackAids: int PTRDict.dict, dfsStartAct: action}

  datatype rooted_msg = ROOTED_MSG of {sender: process_id, msg: msg}

  val proxy = ref (PROXY {context = NONE, source = NONE, sink = NONE})
  val processId = ref ~1

  val emptyW8Vec : w8vec = Vector.tabulate (0, fn _ => 0wx0)
end
