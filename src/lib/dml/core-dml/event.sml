(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure Event : EVENT_INTERNAL =
struct
  open RepTypes
  structure S = CML.Scheduler
  structure O = Orchestrator
  structure GM = GraphManager

  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  datatype z1 = datatype O.caller_kind

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)

  datatype event_info = INFO of {parentAid: action_id, committedRef: bool ref}
  datatype 'a event = EVENT of (event_info -> 'a) list

  fun constructBaseEvent f (INFO {parentAid, committedRef}) =
  let
    val r = f ()
    val _ = S.atomicBegin ()
    val _ =
      if not (!committedRef) then
        (committedRef := true;
        GM.commitChoice parentAid)
      else
        (GM.abortChoice ();
        DmlCore.commit ())
  in
    r
  end

  fun sendEvt (c, m) = EVENT [constructBaseEvent (fn () => DmlCore.send (c, m))]
  fun recvEvt c = EVENT [constructBaseEvent (fn () => DmlCore.recv c)]

  fun wrap (EVENT evts) f = EVENT (List.map (fn g => f o g) evts)

  fun choose evts =
    ListMLton.fold (evts, EVENT [], fn (EVENT l, EVENT acc) => EVENT (l@acc))

  fun syncEvtList evts =
  let
    val _ = S.atomicBegin ()

    val pidInt = !processId
    val tidInt = S.tidInt ()
    val committedRef = ref false
    val resultChan = DmlCore.channel ((Int.toString pidInt)^(Int.toString tidInt)^"_choiceResult")

    fun prolog () = ignore (GM.handleInit {parentAid = NONE})
    fun epilog v = (DmlCore.send (resultChan, v); DmlCore.commit ())

    val _ = ListMLton.map (evts, fn evt =>
      let
        val childTid = S.newTid ()
        val childTidInt = CML.tidToInt childTid
        val {spawnAid, spawnNode = _} = GM.handleSpawn {childTid = ThreadId childTidInt}
        val arg = INFO {parentAid = spawnAid, committedRef = committedRef}
        val childFun = epilog o (fn () => evt arg) o prolog
        val _ = ignore (CML.spawnWithTid (childFun, childTid))
      in
        ()
      end)
    val _ = S.atomicEnd ()

    val result = DmlCore.recv resultChan
  in
    result
  end

  fun sync (EVENT evts: 'a event) =
  let
    (* XXX TODO KC *)
    val _ = DmlCore.commit ()
    val evt = List.nth (evts, 0)
  in
    if length evts = 1 then
      let
        val arg = INFO {parentAid = ActionHelper.dummyAid, committedRef = ref false}
        val r = evt arg
      in
        r
      end
    else syncEvtList evts
  end
end
