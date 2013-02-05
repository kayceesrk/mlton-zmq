(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

open Dml

fun proxy () = startProxy {frontend = "tcp://*:5556", backend = "tcp://*:5557"}

fun arbit () = startArbitrator {sink = "tcp://localhost:5556",
                                source = "tcp://localhost:5557", numPeers = 2}

fun receiver () =
let
  val c1 = channel "c1"
  val c2 = channel "c2"

  fun receiverCore () =
  let
    val _ = recv (c2)
    val _ = recv (c1)
    val _ = recv (c1)
  in
    commit ()
  end

  val _ = connect {sink = "tcp://localhost:5556",
                   source = "tcp://localhost:5557",
                   processId = 1, numPeers = 2}
in
  ignore (runDML (receiverCore, NONE))
end

fun sender () =
let
  val c1 = channel "c1"
  val c2 = channel "c2"

  fun receiver () =
  let
    val _ = recv (c2)
    val _ = recv (c1)
    val _ = recv (c1)
  in
    commit ()
  end

  fun senderCore () =
  let
    val _ = send (c1, 0)
    val _ = send (c2, 0)
    val _ = commit ()
  in
    exitDaemon ()
  end

  val _ = connect {sink = "tcp://localhost:5556",
                   source = "tcp://localhost:5557",
                   processId = 0, numPeers = 2}
in
  ignore (runDML (senderCore, NONE))
end

val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else if args = "arbit" then arbit ()
        else if args = "sender" then sender ()
        else if args = "receiver" then receiver ()
        else raise Fail "Usage: cycle-distr.app <proxy|arbit|sender|receiver>"
