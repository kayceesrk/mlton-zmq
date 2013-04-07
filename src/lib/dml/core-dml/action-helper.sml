(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Action helper
 *
 *)


structure ActionHelper : ACTION_HELPER =
struct
  structure S = CML.Scheduler
  open RepTypes

  (********************************************************************
   * Action
   *******************************************************************)

  fun aidToString (ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid, aid}) =
    concat [Int.toString pid, ".", Int.toString tid, ".", Int.toString rid, ".", Int.toString aid]

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
       | COM => "COM"
       | RB => "RB"

  fun actionToString (ACTION {aid, act}) = concat ["[",aidToString aid,", ",actTypeToString act,"]"]

  fun isAidLocal (ACTION_ID {pid = ProcessId pidInt, ...}) =
    pidInt = (!processId)

  fun getPrevAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid - 1}

  fun getNextAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid + 1}

  fun aidToPtr (ACTION_ID {pid, tid, rid, ...}) = {pid = pid, tid = tid, rid = rid}

  fun actionToAid (ACTION {aid, ...}) = aid
end
