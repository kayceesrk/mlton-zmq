(* rep-types.sml
 * 2013 KC Sivaramakrishnan
 * 2004 Matthew Fluet (mfluet@acm.org)
 *  Ported to MLton threads.
 *)

(* rep-types.sml
 *
 * COPYRIGHT (c) 1995 AT&T Bell Laboratories.
 * COPYRIGHT (c) 1989-1991 John H. Reppy
 *
 * These are the concrete representations of the various CML types.
 * These types are abstract (or not even visible) outside this library.
 *)

structure RepTypes =
   struct

      (** thread IDs --- see thread-id.sml and threads.sml **)
      datatype thread_id = TID of {
            (* a unique ID *)
            id : int,
            (* root-level exception handler hook *)
            exnHandler : (exn -> unit) ref,
            (* hold thread-local properties *)
            props: exn list ref,
            (* state for rollback *)
            actions: exn ResizableArray.t ref,
            (* saved continuation *)
            cont: (unit -> unit) ref,
            (* revision number of the thread; incremented on rollback. *)
            revisionId: int ref,
            (* thread local action identifier generator *)
            actionNum: int ref,
            (* set this whenever this thread does some concurrency operation *)
            done_comm : bool ref
            }

      (** threads --- see scheduler.sml and threads.sml **)
      and 'a thread = THRD of thread_id * 'a MLton.Thread.t
      and rdy_thread = RTHRD of thread_id * MLton.Thread.Runnable.t
   end
