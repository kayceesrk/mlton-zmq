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

fun ponger np =
let
  val pongChan : int chan = channel "ponger"

  fun loop n =
    if n = 0 then
      (commit (); exitDaemon ())
    else (print (concat ["Iteration: ", Int.toString n, "\n"]);
          recv pongChan;
          (* KC: uncommenting next line removes mis-speculations *)
          (* touchLastComm(); *)
          loop (n-1))

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = 1, numPeers = np}
in
  ignore (runDML (fn () => loop ((np-1) * 10), NONE))
end

fun pinger pid np =
let
  val pongChan : int chan = channel "ponger"

  fun loop n =
    if n = 0 then
      (commit (); exitDaemon ())
    else
      (send (pongChan, n);loop (n-1))

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = pid, numPeers = np}
in
  ignore (runDML (fn () => loop 10, NONE))
end

val np = 3

val kind::_ = CommandLine.arguments ()
val _ = if kind = "proxy" then proxy ()
        else if kind = "ponger" then ponger np
        else if kind = "pinger2" then pinger 2 np
        else if kind = "pinger3" then pinger 3 np
        else raise Fail "Unknown argument: Valid arguments are proxy | ponger | pinger(2/3)"
