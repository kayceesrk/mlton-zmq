(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure MatchedComm : MATCHED_COMM =
struct
  open RepTypes
  open GraphManager
  open ActionHelper
  structure S = CML.Scheduler
  structure Debug = LocalDebug(val debug = true)

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (* -------------------------------------------------------------------- *)


  type 'a t = {channel : channel_id, actAid : action_id,
                waitNode : node, value : 'a} AidDict.dict ref

  datatype 'a join_result =
    SUCCESS of {value : 'a, waitNode: GraphManager.node}
  | FAILURE of {actAid : ActionHelper.action_id,
                waitNode : GraphManager.node,
                value : 'a}
  | NOOP

  fun empty () = ref (AidDict.empty)

  fun contains aidDictRef aid = AidDict.member (!aidDictRef) aid

  fun add aidDictRef {channel, actAid, remoteMatchAid, waitNode} value =
  let
    val _ = if isAidLocal remoteMatchAid then raise Fail "MatchedComm.add(1)"
            else if not (isAidLocal actAid) then raise Fail "MatchedComm.add(2)"
            else ()
  in
    aidDictRef := AidDict.insert (!aidDictRef) remoteMatchAid {channel = channel,
                    actAid = actAid, waitNode = waitNode, value = value}
  end

  fun join aidDictRef {remoteAid, withAid, ignoreFailure} =
  let
    val _ = if isAidLocal remoteAid then raise Fail "MatchedComm.join"
            else ()
    val {actAid, waitNode, value, channel = _} = AidDict.lookup (!aidDictRef) remoteAid
    val result =
      if ActionIdOrdered.eq (actAid, withAid) then
        (debug' ("SUCCESS");
          SUCCESS {value = value, waitNode = waitNode})
      else if ignoreFailure then
        raise AidDict.Absent
      else
        (debug' ("FAILURE");
        FAILURE {actAid = actAid, waitNode = waitNode, value = value})
    val _ = aidDictRef := AidDict.remove (!aidDictRef) remoteAid
  in
    result
  end handle AidDict.Absent => (debug' ("NOOP"); NOOP)

  fun cleanup aidDictRef rollbackAids =
  let
    val oldAidDict = !aidDictRef
    val (newAidDict, failList) =
      AidDict.foldl (fn (aid as ACTION_ID {pid, tid, rid, ...} (* key *),
                        failValue as {actAid, waitNode = _ , value = _, channel = _} (* value *),
                        (newAidDict, failList) (* acc *)) =>
        case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
              NONE => (AidDict.insert newAidDict aid failValue, failList)
            | SOME _ =>
                let (* Make sure the blocked (local) thread, is not part of the rollback set *)
                  val ACTION_ID {pid, tid, rid, ...} = actAid
                in
                  case PTRDict.find rollbackAids {pid = pid, tid = tid, rid = rid} of
                      NONE => (AidDict.insert newAidDict aid failValue, failList)
                    | SOME _ => (newAidDict, failValue::failList)
                end) (AidDict.empty, []) oldAidDict
    val _ = aidDictRef := newAidDict
  in
    failList
  end


end

