(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature DML =
sig
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

  val getThreadId : unit -> {pid: int, tid: int}
  val spawn : (unit -> unit) -> unit
  val yield : unit -> unit

  (* Try to commit all previous non speculative communication actions. This
   * operation can ofcourse fail, in which case the program fragment (which
   * inclues all interacted threads) will be rolledback and re-executed
   * non-speculatively. *)
  val commit : unit -> unit

  (* Block the current thread until the previous action has been sated. It is
   * important to note that this only ensure that the last communication is
   * paired up, and does not assure the absence of divergent behavior from
   * synchronous execution. *)
  val touchLastComm : unit -> unit

  (* Quits the communication manager and arbitrator for the calling instance.
   * This ensure that the instance cleanly quits. *)
  val exitDaemon : unit -> unit
end

signature DML_INTERNAL =
sig
  include DML
end
