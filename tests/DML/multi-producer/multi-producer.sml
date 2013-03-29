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

fun ponger () =
let
  val pongChan : int chan = channel "ponger"

  fun loop n =
    if n = 0 then
      (recv pongChan;
       recv pongChan;
       exitDaemon ())
    else (print (concat ["Iteration: ", Int.toString n, "\n"]);
          recv pongChan;
          recv pongChan;
          loop (n-1))

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = 1, numPeers = 3}
in
  ignore (runDML (fn () => loop 3, NONE))
end

fun pinger pid =
let
  val pongChan : int chan = channel "ponger"

  fun loop n =
    if n = ~1 then
      (commit ();
       exitDaemon ())
    else
      let
        val () = send (pongChan, n)
      in
        loop (n-1)
      end

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = pid, numPeers = 3}
in
  ignore (runDML (fn () => loop 3, NONE))
end


val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else if args = "ponger" then ponger ()
        else if args = "pinger2" then pinger 2
        else if args = "pinger3" then pinger 3
        else raise Fail "Unknown argument: Valid arguments are proxy | ponger | pinger(2/3)"
