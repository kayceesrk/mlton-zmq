(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure ZMQ = MLton.ZMQ

fun sender context i =
let
  val socket = ZMQ.sockCreate (context, ZMQ.Pair)
  val _ = if i = "1" then
            ZMQ.sockConnect (socket, "ipc:///tmp/feeds1")
          else if i = "2" then
            ZMQ.sockConnect (socket, "ipc:///tmp/feeds2")
          else raise Fail "Unexpected sender ID"

  (* Handshake *)
  val _ = ZMQ.send (socket, "ping from " ^ i)
  val s : string = ZMQ.recv (socket)
  val _ = print (s ^ "\n")

  (* Main loop *)
  fun loop n =
    if n = 0 then ()
    else (print "sending\n";
          ZMQ.send (socket, n);
          print "sent\n";
          loop (n-1))
  val _ = loop 5

  (* Wait for term *)
  val _ = print (ZMQ.recv (socket))
in
  ZMQ.sockClose socket
end

fun receiver context =
let
  val socket1 = ZMQ.sockCreate (context, ZMQ.Pair)
  val socket2 = ZMQ.sockCreate (context, ZMQ.Pair)
  val _ = ZMQ.sockBind (socket1, "ipc:///tmp/feeds1")
  val _ = ZMQ.sockBind (socket2, "ipc:///tmp/feeds2")

  (* Handshake *)
  fun loop l =
    case l of
         [] => ()
       | _ =>
          let
            val {ins, ...} = ZMQ.poll {ins = l, outs = [], inouts = [], timeout = ~1}
            val socket = List.hd ins
            val s : string = ZMQ.recv (socket)
            val _ = print (s ^ "\n")
            val _ = ZMQ.send (socket, "pong")
            val l = List.filter (fn socket' => not (MLton.equal (socket', socket))) l
          in
            loop l
          end
  val _ = loop [socket1, socket2]

  (* Main loop *)
  fun loop n =
    if n = 0 then ()
    else
      let
        val _ = print "receiving\n"
        val {ins, ...} = ZMQ.poll {ins = [socket1, socket2], outs = [], inouts = [], timeout = ~1}
        val socket = List.hd ins
        val _ = ZMQ.recv (socket) : int
        val _ = print "received\n"
      in
        loop (n-1)
      end
  val _ = loop 10

  (* Wait for term *)
  val _ = ZMQ.send (socket1, "Bye Bye!")
  val _ = ZMQ.send (socket2, "Bye Bye!")
  val _ = ZMQ.sockClose socket1
  val _ = ZMQ.sockClose socket2
in
  ()
end


val args::tail = CommandLine.arguments ()
val context = ZMQ.ctxNew ()
val _ = if args = "receiver" then
          receiver context
        else sender context (hd tail)
