(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure IVar : IVAR =
struct

  open RepTypes

  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)
  structure S = CML.Scheduler
  structure SH = SchedulerHelper

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  datatype 'a k = THREAD of thread_id
                | VALUE of 'a
                | EMPTY

  fun kToString (k) =
    case k of
          THREAD (ThreadId tidInt) => concat ["Thread(", Int.toString tidInt, ")"]
        | EMPTY => "EMPTY"
        | VALUE _ => "VALUE"

  fun new () =
  let
    val r = ref EMPTY
    fun write v =
    let
      val _ = debug' ("IVar.write(1)")
      val _ = S.atomicBegin ()
      val _ = debug (fn () => ("IVar.write: "^(kToString (!r))))
      val _ = case (!r) of
                    EMPTY => r := VALUE v
                  | THREAD (ThreadId tidInt) => (r := VALUE v; SH.resumeThread tidInt emptyW8Vec)
                  | VALUE _ => raise Fail "IVar.read: Filled with value!"
      val _ = S.atomicEnd ()
    in
      ()
    end
    fun read () =
    let
      val _ = Assert.assertNonAtomic' ("IVar.read(1)")
      val _ = debug' ("IVar.read(1)")
      val _ = S.atomicBegin ()
      val _ = debug (fn () => ("IVar.read: "^(kToString (!r))))
      val v = case (!r) of
                    EMPTY =>
                    let
                      val tidInt = S.tidInt ()
                      val _ = r := THREAD (ThreadId tidInt)
                      val _ = SH.blockCurrentThread ()
                    in
                      read ()
                    end
                  | THREAD _ =>
                    let (* KC: If the blocked thread was rolledback, this branch is possible *)
                      val tidInt = S.tidInt ()
                      val _ = r := THREAD (ThreadId tidInt)
                      val _ = SH.blockCurrentThread ()
                    in
                      read ()
                    end
                  | VALUE v => (S.atomicEnd (); v)
      val _ = Assert.assertNonAtomic' ("IVar.read(2)")
    in
      v
    end
  in
    {read = read, write = write}
  end
end
