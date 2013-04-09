(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure PendingComm : PENDING_COMM =
struct

  open RepTypes
  open ActionHelper
  structure S = CML.Scheduler
  structure StrDict = StringSplayDict
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (* -------------------------------------------------------------------- *)

  type 'a t = 'a AidDict.dict StrDict.dict ref

  fun empty () = ref (StrDict.empty)

  fun addAid strDictRef (ChannelId channel) aid value =
  let
    fun merge oldAidDict = AidDict.insert oldAidDict aid value
  in
    strDictRef := StrDict.insertMerge (!strDictRef) channel (AidDict.singleton aid value) merge
  end

  fun removeAid strDictRef (ChannelId channel) aid =
  let
    val aidDict = StrDict.lookup (!strDictRef) channel
    val result = AidDict.lookup aidDict aid
    val aidDict = AidDict.remove aidDict aid
    val _ = strDictRef := StrDict.insert (!strDictRef) channel aidDict
  in
    SOME result
  end handle StrDict.Absent => NONE
           | AidDict.Absent => NONE

  exception FIRST of action_id

  fun deque strDictRef (ChannelId channel) {againstAid} =
  let
    val aidDict = StrDict.lookup (!strDictRef) channel
    fun getOne () =
    let
      val _ = AidDict.app (fn (k, _) =>
                (debug' ("PendingComm.deque: "^(aidToString k));
                if (aidToTidInt k = aidToTidInt againstAid) andalso
                    (aidToPidInt k = aidToPidInt againstAid)
                then () (* dont match actions from the same thread *)
                else raise FIRST k)) aidDict
    in
      raise AidDict.Absent
    end handle FIRST k => k
    val aid = getOne ()
    val return = SOME (aid, AidDict.lookup aidDict aid)
    val _ = removeAid strDictRef (ChannelId channel) aid
  in
    return
  end handle AidDict.Absent => NONE
            | StrDict.Absent => NONE

  fun cleanup strDictRef rollbackAids =
  let
    val _ = debug (fn () => "PendingComm.cleanup: length="^(Int.toString (StrDict.size(!strDictRef))))
    val _ = Assert.assertAtomic' ("PendingComm.cleanup", SOME 1)
    val oldStrDict = !strDictRef
    fun getNewAidDict oldAidDict =
      let
        val emptyAidDict = AidDict.empty
      in
        AidDict.foldl (fn (aid as ACTION_ID {pid, tid, rid, ...}, value, newAidDict) =>
          case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
              NONE => AidDict.insert newAidDict aid value
            | SOME _ => newAidDict) emptyAidDict oldAidDict
      end
    val newStrDict = StrDict.map (fn aidDict => getNewAidDict aidDict) oldStrDict
  in
    strDictRef := newStrDict
  end

  fun contains strDictRef (ChannelId channel) aid =
  let
    val aidDict = StrDict.lookup (!strDictRef) channel
  in
    AidDict.member aidDict aid
  end handle StrDict.Absent => false
end
