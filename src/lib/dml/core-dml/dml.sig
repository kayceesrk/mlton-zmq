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
  include ARBITRATOR
  type 'a chan

  (* ------------------------------------------------------*)
  (* Server *)
  (* ------------------------------------------------------*)

  (* never returns *)
  val startProxy : {frontend: string, backend: string} -> unit

  (* ------------------------------------------------------*)
  (* Clients *)
  (* ------------------------------------------------------*)

  val connect : {sink: string, source: string,
                 processId: int, numPeers: int} -> unit
  val runDML : (unit -> unit) * Time.time option -> OS.Process.status

  val channel : string -> 'a chan
  val send  : 'a chan * 'a -> unit
  val recv  : 'a chan -> 'a
  val spawn : (unit -> unit) -> unit
  val yield : unit -> unit
  val commit : unit -> unit

  val exitDaemon : unit -> unit

  (* ------------------------------------------------------*)
  (* Extra *)
  (* -----
   * For testing purposes only. Should be removed in the release.
   * *)
  (* ------------------------------------------------------*)
end

signature DML_INTERNAL =
sig
  include DML
end
