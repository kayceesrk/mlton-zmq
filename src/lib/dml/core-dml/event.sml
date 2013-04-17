(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
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
  structure AH = ActionHelper
  structure SH = SchedulerHelper

  structure Debug = LocalDebug(val debug = true)

  datatype z1 = datatype O.caller_kind

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  datatype event_info = INFO of {parentAid: action_id,
                                 committedRef: bool ref}
  datatype 'a event = EVENT of (event_info -> 'a) list

  exception NotifySyncThread

  fun constructBaseEvent f (INFO {parentAid, committedRef}) =
  let
    (* Check if the choice has already been matched. If so, kill this thread *)
    val _ = S.atomicBegin ()
    val _ = if (!committedRef) then
              (debug' ("ChoiceHelper: (early) aborting");
               S.atomicEnd ();
               raise CML.Kill)
            else ()

    (* check if the base event has failed *)
    val _ = if GM.firstActionIsRB () andalso
               GM.cacheIsEmpty () then
               (if SH.threadIsAlive parentAid then
                 (S.atomicEnd (); raise NotifySyncThread)
                else (S.atomicEnd (); raise CML.Kill))
            else S.atomicEnd ()

    val r = f ()

    (* Commit or abort choice *)
    val _ = S.atomicBegin ()
    val _ =
      if not (!committedRef) then
        (debug' ("ChoiceHelper: committing");
         committedRef := true;
         CML.tidCompensate (S.getCurThreadId (), fn () => committedRef := false);
         GM.commitChoice parentAid)
      else
        (debug' ("ChoiceHelper: aborting");
         GM.abortChoice ();
         DmlCore.commit ())
  in
    r
  end

  fun sendEvt (c, m) = EVENT [constructBaseEvent (fn () => DmlCore.send (c, m))]
  fun recvEvt c = EVENT [constructBaseEvent (fn () => DmlCore.recv c)]

  fun wrap (EVENT evts) f = EVENT (List.map (fn g => f o g) evts)

  fun choose evts =
    ListMLton.fold (evts, EVENT [], fn (EVENT l, EVENT acc) => EVENT (l@acc))

  fun syncEvtList (evts: (event_info -> 'a) list) =
  let
    val _ = S.atomicBegin ()

    val ptrString = (AH.ptrToString o AH.aidToPtr o AH.actionToAid o GM.getFinalAction) ()
    val resultChan = DmlCore.channel (ptrString^"_choice")
    val committedRef = ref false
    val affId = Random.natLessThan (valOf (Int.maxInt))

    val _ = ListMLton.map (evts, fn evt =>
      let
        val childTid = S.newTidWithAffId affId
        val childTidInt = CML.tidToInt childTid
        val _ = debug' ("Event.syncEvtList: spawning ChoiceHelper thread "^(Int.toString childTidInt))
        val {spawnAid, spawnNode = _} = GM.handleSpawn {childTid = ThreadId childTidInt}

        fun childFun () =
          let
            val _ = ignore (GM.handleInit {parentAid = NONE})
            val _ = O.saveCont ()
            val arg = INFO {parentAid = spawnAid, committedRef = committedRef}
            val v = evt arg
            val _ = DmlCore.commit ()
            val _ = DmlCore.send (resultChan, SOME v)
          in
            ()
          end handle CML.Kill => debug' ("killed self")
                   | NotifySyncThread =>
                       (debug' ("notifying sync thread");
                        DmlCore.send (resultChan, NONE))

        val _ = ignore (CML.spawnWithTid (childFun, childTid))
      in
        ()
      end)

    val _ = S.atomicEnd ()

    val result =
      case DmlCore.recv resultChan of
        SOME v => v
      | NONE => (debug' "retry choice"; syncEvtList evts)

    (* KC: This commit will immediately succeed and will not block since the
     * event picked in the choice just committed. However, this commit is
     * important since it precludes any spawns (associated with the event
     * synchronization), to be part of the rollback log (cache)
     *)
    val _ = DmlCore.commit ()
  in
    result
  end

  fun sync (EVENT evts: 'a event) : 'a =
  let
    val _ = debug' ("sync")
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
