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
  val pingChan : int chan = channel "pinger"

  fun loop n =
    if n = 0 then
      (print ("Sending 1st ~1\n");
       send (pingChan, ~1);
       print ("Sending 2nd ~1\n");
       send (pingChan, ~1);
       commit ();
       exitDaemon ())
    else (print (concat ["Iteration: ", Int.toString n, "\n"]);
          send (pingChan, 2*n);
          send (pingChan, 2*n-1);
          loop (n-1))

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = 1, numPeers = 3}
in
  ignore (runDML (fn () => loop 2, NONE))
end

fun ponger pid =
let
  val pingChan : int chan = channel "pinger"

  fun loop n =
    if n = ~1 then (commit (); exitDaemon ())
    else
      let
        val v = recv pingChan
        (* KC: uncommenting next line removes mis-speculations *)
        val _ = touchLastComm ()
        val _ = print (concat ["Got value: ", Int.toString v, "\n"])
      in
        loop v
      end

  val _ = connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557",
                   processId = pid, numPeers = 3}
in
  ignore (runDML (fn () => loop 0, NONE))
end


val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then proxy ()
        else if args = "pinger" then pinger ()
        else if args = "ponger2" then ponger 2
        else if args = "ponger3" then ponger 3
        else raise Fail "Unknown argument: Valid arguments are proxy | pinger | ponger(2/3)"
