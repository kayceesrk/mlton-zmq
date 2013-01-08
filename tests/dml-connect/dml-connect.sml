(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

val args::_ = CommandLine.arguments ()
val _ = if args = "proxy" then
          Dml.startProxy {frontend = "tcp://*:5556", backend = "tcp://*:5557"}
        else
          (Dml.connect {sink = "tcp://localhost:5556", source = "tcp://localhost:5557"};
          print "Done\n")
