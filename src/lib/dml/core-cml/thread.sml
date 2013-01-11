(* thread.sml
 * 2004 Matthew Fluet (mfluet@acm.org)
 *  Ported to MLton threads.
 *)

(* thread.sml
 *
 * COPYRIGHT (c) 1995 AT&T Bell Laboratories.
 * COPYRIGHT (c) 1989-1991 John H. Reppy
 *)

structure Thread : THREAD =
   struct
      structure Assert = LocalAssert(val assert = false)
      structure Debug = LocalDebug(val debug = false)

      structure S = Scheduler
      fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
      fun debug' msg = debug (fn () => msg)

      open ThreadID

      fun generalExit (tid', _) =
         let
            val () = Assert.assertNonAtomic' "Thread.generalExit"
            val () = debug' "generalExit" (* NonAtomic *)
            val () = Assert.assertNonAtomic' "Thread.generalExit"
         in
            S.switchToNext
            (fn t =>
             let
                val tid = S.getThreadId t
                val () = Assert.assert ([], fn () =>
                                        concat ["Thread.generalExit ",
                                                Option.getOpt (Option.map tidToString tid', "NONE"),
                                                " <> ",
                                                tidToString tid], fn () =>
                                         case tid' of NONE => true
                                          | SOME tid' => sameTid (tid', tid))
             in
                ()
             end)
         end

      fun doHandler (TID {exnHandler, ...}, exn) =
         (debug (fn () => concat ["Exception: ", exnName exn, " : ", exnMessage exn])
          ; ((!exnHandler) exn) handle _ => ())

      fun spawnc f x =
         let
            val () = S.atomicBegin ()
            fun thread tid () =
               (ignore (f x) handle ex => doHandler (tid, ex)
               ; generalExit (SOME tid, false))
            val t = S.new thread
            val tid = S.getThreadId t
            val () = S.ready (S.prep t)
            val () = S.atomicEnd ()
            val () = debug (fn () => concat ["spawnc ", tidToString tid])  (* NonAtomic *)
         in
            tid
         end
      fun spawn f = spawnc f ()

      val getTid = S.getCurThreadId

      fun exit () =
         let
            val () = Assert.assertNonAtomic' "Thread.exit"
            val () = debug' "exit" (* NonAtomic *)
            val () = Assert.assertNonAtomic' "Thread.exit"
         in
            generalExit (NONE, true)
         end

      fun yield () =
         let
            val () = Assert.assertNonAtomic' "Thread.yield"
            (* val () = debug' "yield" (* NonAtomic *) *)
            val () = Assert.assertNonAtomic' "Thread.yield"
         in
            S.readyAndSwitchToNext (fn () => ())
         end

    end
