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

fun pinger () =
let
  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", nodeId = 1}
  fun spawner () =
  let
    val foo = fn () => (print "Hello from node 1"; exitLocal ())
    val _ = spawnRemote {thunk = foo, nodeId = 2}
  in
    exitLocal ()
  end
in
  ignore (runDML (spawner, NONE))
end

fun ponger () =
let
  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", nodeId = 2}
in
  ignore (runDML (fn () => (), NONE))
end


val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else if args = "pinger" then pinger ()
        else if args = "ponger" then ponger ()
        else raise Fail "Unknown argument: Valid arguments are proxy | pinger | ponger"
