(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Action helper
 *
 *)


structure ActionManager : ACTION_MANAGER =
struct
  structure S = CML.Scheduler
  open RepTypes

  (********************************************************************
   * Action
   *******************************************************************)

  fun aidToString (ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid, aid}) =
    concat [Int.toString pid, ":", Int.toString tid, ":", Int.toString rid, ":", Int.toString aid]

  fun dummyAid () = ACTION_ID {pid = ProcessId (!processId), tid = ThreadId ~1, rid = ~1, aid = ~1}

  fun newAid () =
  let
    val pid = !processId
    val tid = S.tidInt ()
    val aid = S.tidNextActionNum ()
    val rid = S.tidRev ()
  in
    ACTION_ID {pid = ProcessId pid, tid = ThreadId tid, rid = rid, aid = aid}
  end

  fun aidToPidInt (ACTION_ID {pid = ProcessId pidInt, ...}) = pidInt
  fun aidToTidInt (ACTION_ID {tid = ThreadId tidInt, ...}) = tidInt
  fun aidToTid (ACTION_ID {tid, ...}) = tid
  fun aidToActNum (ACTION_ID {aid, ...}) = aid

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
       | COM_RB => "CR"

  fun actionToString (ACTION {aid, act}) = concat ["[",aidToString aid,", ",actTypeToString act,"]"]

  fun isAidLocal (ACTION_ID {pid = ProcessId pidInt, ...}) =
    pidInt = (!processId)

  fun getPrevAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid - 1}

  fun getNextAid (ACTION_ID {pid, tid, rid, aid}) =
    ACTION_ID {pid = pid, tid = tid, rid = rid, aid = aid + 1}


  structure ActionIdOrdered
    :> ORDERED where type t = action_id
  = struct
    type t = action_id

    fun eq (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, aid = aid1},
            ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, aid = aid2}) =
            pid1 = pid2 andalso tid1 = tid2 andalso rid1 = rid2 andalso aid1 = aid2

    fun compare (ACTION_ID {pid = ProcessId pid1, tid = ThreadId tid1, rid = rid1, aid = aid1},
                 ACTION_ID {pid = ProcessId pid2, tid = ThreadId tid2, rid = rid2, aid = aid2}) =
      (case Int.compare (pid1, pid2) of
           EQUAL => (case Int.compare (tid1, tid2) of
                          EQUAL => (case Int.compare (rid1, rid2) of
                                         EQUAL => Int.compare (aid1, aid2)
                                       | lg => lg)
                        | lg => lg)
         | lg => lg)
  end

  structure ActionIdSplaySet = SplaySet (structure Elem = ActionIdOrdered)
  structure AISS = ActionIdSplaySet
  structure ActionIdSplayDict = SplayDict (structure Key = ActionIdOrdered)
  structure AISD = ActionIdSplayDict

  structure PTROrdered :> ORDERED where type t = ptr =
  struct
    type t = ptr

    val eq = MLton.equal
    fun compare ({pid = ProcessId pidInt1, tid = ThreadId tidInt1, rid = rid1},
                 {pid = ProcessId pidInt2, tid = ThreadId tidInt2, rid = rid2}) =
      (case Int.compare (pidInt1, pidInt2) of
            EQUAL => (case Int.compare (tidInt1, tidInt2) of
                           EQUAL => Int.compare (rid1, rid2)
                         | lg => lg)
          | lg => lg)
  end

  structure PTRDict = SplayDict (structure Key = PTROrdered)

  fun aidToPtr (ACTION_ID {pid, tid, rid, ...}) = {pid = pid, tid = tid, rid = rid}
end
