(* cml.sml
 * 2004 Matthew Fluet (mfluet@acm.org)
 *  Ported to MLton threads.
 *)

(* cml.sml
 *
 * COPYRIGHT (c) 1995 AT&T Bell Laboratories.
 * COPYRIGHT (c) 1989-1991 John H. Reppy
 *)

structure CML : CML =
   struct
      open RepTypes
      structure Scheduler = Scheduler
      open Thread
   end
