(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure ZMQ = MLton.ZMQ

fun sender socket =
let
  val _ = ZMQ.sockBind (socket, "ipc:///tmp/feeds1")
  val prefix = MLton.serialize ("PREFIX")
  val _ = ZMQ.sendWithPrefix (socket, "MSG", prefix)
  val d : string = ZMQ.recv (socket)
in
  ZMQ.sockClose socket
end

fun receiver socket =
let
  val _ = ZMQ.sockConnect (socket, "ipc:///tmp/feeds1")
  val msg : string = ZMQ.recv (socket)
  val _ = print (msg ^ "\n")
  val _ = ZMQ.send (socket, "Bye Bye!")
in
  ZMQ.sockClose socket
end

val args::_ = CommandLine.arguments ()
val context = ZMQ.ctxNew ()
val socket = ZMQ.sockCreate (context, ZMQ.Pair)
val _ = if args = "sender" then sender socket else receiver socket
