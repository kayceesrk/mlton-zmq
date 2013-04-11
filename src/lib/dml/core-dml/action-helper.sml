(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure ActionHelper : ACTION_HELPER =
struct
  structure S = CML.Scheduler
  open RepTypes

  (********************************************************************
   * Action
   *******************************************************************)

  fun aidToString (ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid, aid}) =
    concat [Int.toString pid, ".", Int.toString tid, ".",
            Int.toString rid, ".", Int.toString aid]

  fun ptrToString ({pid = ProcessId pid, tid = ThreadId tid, rid}) =
    concat [Int.toString pid, ":", Int.toString tid, ":", Int.toString rid]

  fun newAid () =
  let
    val pid = !processId
    val tid = S.tidInt ()
    val aid = S.tidNextActionNum ()
    val rid = S.tidRev ()
  in
    ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid = rid, aid = aid}
  end

  val dummyAid = ACTION_ID {pid = ProcessId ~1, tid = ThreadId ~1, rid = ~1, aid = ~1}

  fun aidToPidInt (ACTION_ID {pid = ProcessId pidInt, ...}) = pidInt
  fun aidToTidInt (ACTION_ID {tid = ThreadId tidInt, ...}) = tidInt
  fun aidToActNum (ACTION_ID {aid, ...}) = aid
  (* fun aidToPid (ACTION_ID {pid, ...}) = pid *)
  fun aidToTid (ACTION_ID {tid, ...}) = tid

  fun actTypeToString at =
    case at of
         SEND_WAIT {cid = ChannelId cstr, matchAid = NONE} => concat ["SW(",cstr,",*)"]
       | SEND_WAIT {cid = ChannelId cstr, matchAid = SOME act} => concat ["SW(",cstr,",",aidToString act,")"]
       | SEND_ACT {cid = ChannelId cstr} => concat ["SA(", cstr, ")"]
       | RECV_WAIT {cid = ChannelId cstr, matchAid = NONE} => concat ["RW(",cstr,",*)"]
       | RECV_WAIT {cid = ChannelId cstr, matchAid = SOME act} => concat ["RW(",cstr,",",aidToString act,")"]
       | RECV_ACT {cid = ChannelId cstr} => concat ["RA(", cstr, ")"]
       | BEGIN {parentAid} => concat ["B(", aidToString parentAid, ")"]
       | SPAWN {childTid = ThreadId tid} => concat ["F(", Int.toString tid, ")"]
       | NOOP => "NOOP"
       | COM => "COM"
       | RB => "RB"

  fun actionToString axn =
    case axn of
         ACTION {aid, act} => concat ["[",aidToString aid,", ",actTypeToString act,"]"]
       | EVENT {actions, ...} =>
           let
             val prolog = "Event["
             val body = AidDict.foldr (fn (k,v,str) => (actionToString (ACTION{aid = k, act = v})^str)) "" actions
             val epilog = "]"
           in
             concat [prolog, body, epilog]
           end


  fun isAidLocal (ACTION_ID {pid = ProcessId pidInt, ...}) =
    pidInt = (!processId)

  (* fun getPrevAid (ACTION_ID {pid, tid, rid, aid, vid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid - 1, vid = vid} *)

  fun getNextAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid + 1}

  fun aidToPtr (ACTION_ID {pid, tid, rid, ...}) = {pid = pid, tid = tid, rid = rid}

  fun actionToAid axn =
    case axn of
         ACTION {aid, ...} => aid
       | _ => raise Fail "ActionHelper.actionToAid: saw event"

  fun actNumPlus (ACTION_ID {pid, tid, rid, aid}) inc =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid + inc}
end
