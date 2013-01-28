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
  val rolledBack = ref false
  fun core () =
  let
    val c1 : int chan = channel "chan1"
    val _ = send (c1, 0)
  in
    if !rolledBack then exitDaemon () else (rolledBack := true; rollback ())
  end

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", processId = 1}
in
  ignore (runDML (core, NONE))
end

fun ponger () =
let
  val rolledBack = ref false

  fun core () =
  let
    val c1 : int chan = channel "chan1"
    val c2 : unit chan = channel "chan2"
    val _ = recv c1
    val _ = if !rolledBack then exitDaemon () else (rolledBack := true; recv c2)
  in
    ()
  end

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", processId = 2}
in
  ignore (runDML (core, NONE))
end


val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else if args = "pinger" then pinger ()
        else if args = "ponger" then ponger ()
        else raise Fail "Unknown argument: Valid arguments are proxy | pinger | ponger"
