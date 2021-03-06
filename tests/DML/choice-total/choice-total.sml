(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

open Dml

fun proxy () = startProxy {frontend = "tcp://*:5556", backend = "tcp://*:5557"}

fun client () =
let
  val c1 = channel "c1"
  val c2 = channel "c2"

  fun newThread () =
  let
    val e1 = wrap (sendEvt (c2, 2)) (fn () =>
              let
                val _ = print "picked send on c2\n"
                val _ = recv c1
                val _ = print "finished recv on c1\n"
              in
                ()
              end)
    val e2 = wrap (recvEvt c1) (fn _ =>
              let
                val _ = print "picked recv on c1\n"
                val _ = send (c2, 2)
                val _ = print "finished send on c2\n"
              in
                ()
              end)
    val e = choose [e2,e1]
    val _ = sync e
    val _ = commit ()
  in
    ()
  end

  fun core () =
  let
    val _ = spawn newThread
    val e1 = wrap (sendEvt (c1, 1)) (fn () =>
              let
                val _ = print "picked send on c1\n"
                val _ = recv c2
                val _ = print "finished recv on c2\n"
              in
                ()
              end)
    val e2 = wrap (recvEvt c2) (fn _ =>
              let
                val _ = print "picked recv on c2\n"
                val _ = send (c1, 1)
                val _ = print "finished send on c1\n"
              in
                ()
              end)
    val e = choose [e2,e1]
    val _ = sync e
    val _ = commit ()
  in
    exitDaemon ()
  end

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = 1, numPeers = 1}
in
  ignore (runDML (core, NONE))
end

val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else client ()
