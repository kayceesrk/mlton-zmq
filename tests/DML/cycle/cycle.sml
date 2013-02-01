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

fun client () =
let
  val c1 = channel "c1"
  val c2 = channel "c2"

  fun receiver () =
  let
    val _ = recv (c2)
    val _ = recv (c1)
  in
    ()
  end

  fun core () =
  let
    val _ = spawn (receiver)
    val _ = send (c1, 0)
    val _ = send (c2, 0)
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
