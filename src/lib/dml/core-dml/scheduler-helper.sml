(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure SchedulerHelper =
struct

  open RepTypes
  open ActionHelper

  structure S = CML.Scheduler
  structure IntDict = IntSplayDict
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (* -------------------------------------------------------------------- *)
  (* State *)
  (* -------------------------------------------------------------------- *)

  val blockedThreads : w8vec S.thread IntDict.dict ref = ref (IntDict.empty)
  (* Commit function - Will be updated by dml-core *)
  val commitRef = ref (fn () => ())

  (* -------------------------------------------------------------------- *)

  fun blockCurrentThread () =
  let
    val _ = Assert.assertAtomic' ("DmlDecentalized.blockCurrentThread", SOME 1)
    val tidInt = S.tidInt ()
  in
    S.atomicSwitchToNext (fn t => blockedThreads := IntDict.insert (!blockedThreads) tidInt t)
  end

  fun resumeThread tidInt (value : w8vec) =
  let
    val _ = Assert.assertAtomic' ("DmlCore.unblockthread", NONE)
    val t = IntDict.lookup (!blockedThreads) tidInt
    val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
    val rt = S.prepVal (t, value)
  in
    S.ready rt
  end handle IntDict.Absent => ()

  fun rollbackBlockedThreads ptrDict =
  let
    val _ = Assert.assertAtomic' ("DmlCore.rollbackBlockedThreads", SOME 1)
    fun rollbackBlockedThread t actNum =
      let
        val prolog = fn () => (GraphManager.restoreCont actNum; emptyW8Vec)
        val rt = S.prep (S.prepend (t, prolog))
        val _ = S.ready rt
      in
        ()
      end
    val newBTDict =
      IntDict.foldl (fn (tidInt, t as S.THRD (tid, _), newBTDict) =>
        let
          val pid = ProcessId (!processId)
          val rid = CML.tidToRev tid
          val tid = ThreadId tidInt
        in
          case PTRDict.find ptrDict {pid = pid, tid = tid, rid = rid} of
                NONE => IntDict.insert newBTDict tidInt t
              | SOME actNum => (rollbackBlockedThread t actNum; newBTDict)
        end) IntDict.empty (!blockedThreads)
  in
    blockedThreads := newBTDict
  end

  fun rollbackReadyThreads ptrDict =
  let
    val _ = Assert.assertAtomic' ("DmlCore.rollbackReadyThreads", SOME 1)
    fun restoreSCore (rthrd as S.RTHRD (cmlTid, _)) =
      let
        val pid = ProcessId (!processId)
        val rid = CML.tidToRev cmlTid
        val tid = ThreadId (CML.tidToInt cmlTid)
      in
        case PTRDict.find ptrDict {pid = pid, tid = tid, rid = rid} of
             NONE => rthrd
           | SOME actNum => S.RTHRD (cmlTid, MLton.Thread.prepare
              (MLton.Thread.new (fn () => GraphManager.restoreCont actNum), ()))
      end
  in
    S.modify restoreSCore
  end

  fun forceCommit (node) =
  let
    val aid = (actionToAid o GraphManager.nodeToAction) node
    val tidInt = aidToTidInt aid

    val _ = Assert.assertAtomic'("forceCommit(1)", NONE)
    val _ = debug' ("forceCommit: "^(aidToString aid))

    exception DONE

    fun handleBlockedThread () =
    let
      val t = IntDict.lookup (!blockedThreads) tidInt
      val _ = blockedThreads := IntDict.remove (!blockedThreads) tidInt
      fun prolog () = ((!commitRef) (); emptyW8Vec)
      val rt = S.prep (S.prepend (t, prolog))
    in
      S.ready rt
    end handle IntDict.Absent => handleReadyThread ()
             | DONE => ()

    and handleReadyThread () =
    let
      fun core (rthrd as S.RTHRD (cmlTid, _)) =
      let
        val pid = ProcessId (!processId)
        val rid = CML.tidToRev cmlTid
        val tid = ThreadId (CML.tidToInt cmlTid)
      in
        if MLton.equal (aidToPtr aid, {pid = pid, tid = tid, rid = rid}) then
           S.RTHRD (cmlTid, MLton.Thread.prepare (MLton.Thread.new (!commitRef), ()))
        else rthrd
      end
    in
      S.modify core
    end
  in
    handleBlockedThread ()
  end

  fun threadIsAlive aid =
  let
    val S.THRD (cmlTid, _) = IntDict.lookup (!blockedThreads) (aidToTidInt aid)
  in
    CML.tidToRev cmlTid = (aidToRidInt aid)
  end handle IntDict.Absent => false
end
