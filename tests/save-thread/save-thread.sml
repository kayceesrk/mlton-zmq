(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

open Dml

fun client () =
let
  fun loop n =
    if n = 0 then ()
    else (if n mod 100000 = 0 then
            (print (concat [(Int.toString n),"\n"]);
            yield ();
            loop (n-1))
          else loop (n-1))
  val _ = print "Running client\n"
  val _ = spawn (fn () => loop 10000000)
  val _ = yield ()
  val w = save ()
  val _ = (case w of
               NONE => print "Done\n"
             | SOME w => (print "Saved\n"; yield (); print "Restoring\n"; restore w))
in
  ()
end

val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then
          startProxy {frontend = "tcp://*:5556", backend = "tcp://*:5557"}
        else
          (connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557", nodeId = 1};
           ignore (runDML (client, SOME (Time.fromSeconds (Int.toLarge 100)))))
