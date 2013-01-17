(* thread-id.sig
* 2004 Matthew Fluet (mfluet@acm.org)
*  Ported to MLton threads.
*)

(* threads-sig.sml
*
* COPYRIGHT (c) 1995 AT&T Bell Laboratories.
* COPYRIGHT (c) 1989-1991 John H. Reppy
*)

signature THREAD_ID =
sig
  type thread_id

  val sameTid    : (thread_id * thread_id) -> bool
  val compareTid : (thread_id * thread_id) -> order
  val hashTid    : thread_id -> word

  val tidToString : thread_id -> string
  val tidToInt : thread_id -> int
  val tidToRev : thread_id -> int
  val tidToNode : thread_id -> unit DirectedGraph.Node.t option ref
  val tidNextActionNum : thread_id -> int

  val tidSaveCont    : thread_id * (unit -> unit) -> unit
  val tidRestoreCont : thread_id -> unit
end

signature THREAD_ID_EXTRA =
sig
  datatype thread_id' = datatype RepTypes.thread_id
  include THREAD_ID where type thread_id = thread_id'
  val new : unit -> thread_id
  val bogus : string -> thread_id
  val reset : unit -> unit
end
