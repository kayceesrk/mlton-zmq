(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure MessageFilter =
struct
  open RepTypes
  structure S = CML.Scheduler
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = false)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)

  structure PTOrdered :> ORDERED
    where type t = {pid: process_id, tid: thread_id} =
    struct
      type t = {pid: process_id, tid: thread_id}

      val eq = MLton.equal
      val _ = eq

      fun compare ({pid = ProcessId pidInt1, tid = ThreadId tidInt1},
                    {pid = ProcessId pidInt2, tid = ThreadId tidInt2}) =
        case Int.compare (pidInt1, pidInt2) of
              EQUAL => Int.compare (tidInt1, tidInt2)
            | lg => lg
    end

  structure PTDict = SplayDict (structure Key = PTOrdered)

  val filterRef = ref PTDict.empty

  fun addToFilter rollbackAids =
    let
      val _ = Assert.assertAtomic' ("MessageFilter.addFilter", NONE)
      val newFilter = ListMLton.fold (PTRDict.domain rollbackAids, PTDict.empty,
        fn ({pid, tid, rid}, newFilter) => PTDict.insert newFilter {pid = pid, tid = tid} rid)
      val oldFilter = !filterRef
      val newFilter = PTDict.union oldFilter newFilter (fn (_,i,j) => if i>j then i else j)
    in
      filterRef := newFilter
    end

  fun isAllowed (aid as ACTION_ID {pid, tid, rid, ...}) =
    case PTDict.find (!filterRef) {pid = pid, tid = tid} of
          NONE =>
          let
            val _ = debug (fn () => "MessageFilter: blocking aid="^(ActionHelper.aidToString aid))
          in
            true
          end
        | SOME rid' =>
            if rid <= rid' then false
            else (* if rid > rid', then we remove the entry from filter *)
              let
                val ProcessId pidInt = pid
                val ThreadId tidInt = tid
                val _ = debug (fn () => "MessageFilter: removing filter (pid="^(Int.toString pidInt)
                                      ^",tid="^(Int.toString tidInt)^")")
                val _ = filterRef := PTDict.remove (!filterRef) {pid = pid, tid = tid}
              in
                true
              end
end
