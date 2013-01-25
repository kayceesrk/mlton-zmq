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

  val processId = ref ~1
end
