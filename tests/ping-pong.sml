(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure ZMQ = MLton.ZMQ

fun pinger socket =
let
  val _ = ZMQ.sockBind (socket, "ipc:///tmp/feeds0")

  (* Handshake *)
  val _ = ZMQ.send (socket, "ping")
  val s : string = ZMQ.recv (socket)
  val _ = print (s ^ "\n")

  (* Main loop *)
  fun loop n =
    if n = 0 then ()
    else (print "sending\n";
          ZMQ.send (socket, n);
          print "sent\n";
          loop (n-1))
  val _ = loop 100

  (* Wait for term *)
  val _ = print (ZMQ.recv (socket))
in
  ZMQ.sockClose socket
end

fun ponger socket =
let
  val _ = ZMQ.sockConnect (socket, "ipc:///tmp/feeds0")

  (* Handshake *)
  val s : string = ZMQ.recv socket
  val _ = print (s ^ "\n")
  val _ = ZMQ.send (socket, "pong")

  (* Main loop *)
  fun loop n =
    if n = 0 then ()
    else (print "receiving\n";
          ZMQ.recv (socket) : int;
          print "received\n";
          loop (n-1))
  val _ = loop 100

  (* Wait for term *)
  val _ = ZMQ.send (socket, "Bye Bye!")
in
  ZMQ.sockClose socket
end


val args::_ = CommandLine.arguments ()
val context = ZMQ.ctxNew ()
val socket = ZMQ.sockCreate (context, ZMQ.Pair)
val _ = if args = "pinger" then pinger socket else ponger socket
