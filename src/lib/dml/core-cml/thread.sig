(* thread.sig
 * 2004 Matthew Fluet (mfluet@acm.org)
 *  Ported to MLton threads.
 *)

(* threads-sig.sml
 *
 * COPYRIGHT (c) 1995 AT&T Bell Laboratories.
 * COPYRIGHT (c) 1989-1991 John H. Reppy
 *)

signature THREAD =
  sig
     include THREAD_ID
     exception Exit
     val getTid : unit -> thread_id

     val spawnc : ('a -> unit) -> 'a -> thread_id
     val spawn  : (unit -> unit) -> thread_id
     val exit   : unit -> 'a
     val yield  : unit -> unit  (* mostly for benchmarking *)
  end

