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

fun pinger () =
let
  val pingChan : int chan = channel "pinger"
  val pongChan : int chan = channel "ponger"

  fun loop n =
    if n = 0 then exitDaemon ()
    else (print (concat ["Iteration: ", Int.toString n, "\n"]);
          send (pingChan, n);
          ignore (recv pongChan);
          loop (n-1))

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", processId = 1}
in
  ignore (runDML (fn () => loop 100, NONE))
end

fun ponger () =
let
  val pingChan : int chan = channel "pinger"
  val pongChan : int chan = channel "ponger"

  fun loop n =
    if n = 0 then exitDaemon ()
    else (print (concat ["Iteration: ", Int.toString n, "\n"]);
          ignore (recv pingChan);
          send (pongChan, n);
          loop (n-1))

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", processId = 2}
in
  ignore (runDML (fn () => loop 100, NONE))
end


val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else if args = "pinger" then pinger ()
        else if args = "ponger" then ponger ()
        else raise Fail "Unknown argument: Valid arguments are proxy | pinger | ponger"
