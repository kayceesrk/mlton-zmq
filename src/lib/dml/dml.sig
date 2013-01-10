(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature DML =
sig
  type chan

  (* ------------------------------------------------------*)
  (* Server *)
  (* ------------------------------------------------------*)

  (* never returns *)
  val startProxy : {frontend: string, backend: string} -> unit

  (* ------------------------------------------------------*)
  (* Clients *)
  (* ------------------------------------------------------*)

  val connect : {sink: string, source: string, nodeId: int} -> unit
  val runDML : (unit -> unit) * Time.time option -> OS.Process.status

  val channel : string -> chan
  val send : chan * Word8.word vector -> unit
  val recv : chan -> Word8.word vector

  (* ------------------------------------------------------*)
end
