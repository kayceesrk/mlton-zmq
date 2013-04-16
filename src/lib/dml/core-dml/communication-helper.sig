(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature COMMUNICATION_MANAGER =
sig
  type msg = RepTypes.msg

  val msgToString : msg -> string
  val msgSend     : msg -> unit
  val msgSendSafe : msg -> unit
  val msgRecv     : unit -> msg option
  val msgRecvSafe : unit -> msg option
end
