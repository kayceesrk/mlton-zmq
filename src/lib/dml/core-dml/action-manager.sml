(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)


structure ActionManager : ACTION_MANAGER =
struct
  structure S = CML.Scheduler
  structure RA = ResizableArray
  open RepTypes

  (********************************************************************
   * Debug
   *******************************************************************)

  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)


  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  datatype node = NODE of {array: exn ResizableArray.t, index: int}

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


  datatype action_type = SEND_WAIT of {cid: channel_id, matchAid: action_id option}
                       | SEND_ACT of {cid: channel_id}
                       | RECV_WAIT of {cid: channel_id, matchAid: action_id option}
                       | RECV_ACT of {cid: channel_id}
                       | SPAWN of {childTid: thread_id}
                       | BEGIN of {parentAid: action_id}
                       | COM_RB (* This indicates the node that is inserted after commit or rollback *)

  fun aidToPidInt (ACTION_ID {pid = ProcessId pidInt, ...}) = pidInt
  fun aidToTidInt (ACTION_ID {tid = ThreadId tidInt, ...}) = tidInt
  fun aidToTid (ACTION_ID {tid, ...}) = tid

  fun actTypeToString at =
    case at of
         SEND_WAIT {cid = ChannelId cstr, matchAid = NONE} => concat ["SW (",cstr,",*)"]
       | SEND_WAIT {cid = ChannelId cstr, matchAid = SOME act} => concat ["SW (",cstr,",",aidToString act,")"]
       | SEND_ACT {cid = ChannelId cstr} => concat ["SA (", cstr, ")"]
       | RECV_WAIT {cid = ChannelId cstr, matchAid = NONE} => concat ["RW (",cstr,",*)"]
       | RECV_WAIT {cid = ChannelId cstr, matchAid = SOME act} => concat ["RW (",cstr,",",aidToString act,")"]
       | RECV_ACT {cid = ChannelId cstr} => concat ["RA (", cstr, ")"]
       | BEGIN {parentAid} => concat ["B (", aidToString parentAid, ")"]
       | SPAWN {childTid = ThreadId tid} => concat ["F(", Int.toString tid, ")"]
       | COM_RB => "CR"

  datatype action = ACTION of {aid: action_id, act: action_type}

  fun actionToString (ACTION {aid, act}) = concat ["[",aidToString aid,",",actTypeToString act,"]"]

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

  (********************************************************************
   * Node management
   *******************************************************************)

  exception NodeExn of action

  fun handleInit {parentAid : action_id} =
  let
    val actions = S.tidActions ()
    val beginAid = newAid ()
    val act = ACTION {aid = beginAid, act = BEGIN {parentAid = parentAid}}
    val _ = RA.addToEnd (actions, NodeExn act)
  in
    beginAid
  end

  (* Must be called by spawning thread. Adds \po edge.
   * Returns: a new begin node.
   * *)
  fun handleSpawn {childTid} =
  let
    val actions = S.tidActions ()
    val spawnAid = newAid ()
    val spawnAct = ACTION {aid = spawnAid, act = SPAWN {childTid = childTid}}
    val _ = RA.addToEnd (actions, NodeExn spawnAct)
  in
    spawnAid
  end

  fun insertCommitRollbackNode () =
  let
    val actions = S.tidActions ()
    val comRbAid = newAid ()
    val act = ACTION {aid = comRbAid, act = COM_RB}
    val _ = RA.addToEnd (actions, NodeExn act)
  in
    comRbAid
  end


  fun handleSend {cid: channel_id} =
  let
    val actions = S.tidActions ()
    (* act *)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = SEND_ACT {cid = cid}}
    val _ = RA.addToEnd (actions, NodeExn actAct)
    (* wait *)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = SEND_WAIT {cid = cid, matchAid = NONE}}
    val _ = RA.addToEnd (actions, NodeExn waitAct)
    val waitNode = NODE {array = actions, index = (RA.length actions) - 1}
  in
    {waitNode = waitNode, actAid = actAid}
  end

  fun handleRecv {cid: channel_id} =
  let
    val actions = S.tidActions ()
    (* act *)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = RECV_ACT {cid = cid}}
    val _ = RA.addToEnd (actions, NodeExn actAct)
    (* wait *)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = RECV_WAIT {cid = cid, matchAid = NONE}}
    val _ = RA.addToEnd (actions, NodeExn waitAct)
    val waitNode = NODE {array = actions, index = (RA.length actions) - 1}
  in
    {waitNode = waitNode, actAid = actAid}
  end

  fun getActionFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn act => act
       | _ => raise Fail "getActionFromArrayAtIndex"

  fun setMatchAid (NODE {array, index}) (matchAid: action_id) =
  let
    val (ACTION {aid, act}) = getActionFromArrayAtIndex (array, index)
    val newAct = case act of
                      SEND_WAIT {cid, matchAid = NONE} => SEND_WAIT {cid = cid, matchAid = SOME matchAid}
                    | RECV_WAIT {cid, matchAid = NONE} => RECV_WAIT {cid = cid, matchAid = SOME matchAid}
                    | _ => raise Fail "ActionManager.setMatchAid"
  in
    RA.update (array, index, NodeExn (ACTION {aid = aid, act = newAct}))
  end

  fun removeMatchAid (NODE {array, index}) =
  let
    val (ACTION {aid, act}) = getActionFromArrayAtIndex (array, index)
    val newAct = case act of
                      SEND_WAIT {cid, ...} => SEND_WAIT {cid = cid, matchAid = NONE}
                    | RECV_WAIT {cid, ...} => RECV_WAIT {cid = cid, matchAid = NONE}
                    | _ => raise Fail "ActionManager.removeMatchAid"
  in
    RA.update (array, index, NodeExn (ACTION {aid = aid, act = newAct}))
  end

  fun getMatchAid (NODE {array, index}) =
  let
    val (ACTION {act, ...}) = getActionFromArrayAtIndex (array, index)
    val matchAid = case act of
                      SEND_WAIT {matchAid = SOME mAid, ...} => mAid
                    | RECV_WAIT {matchAid = SOME mAid, ...} => mAid
                    | _ => raise Fail "ActionManager.getMatchAid"
  in
    matchAid
  end

  fun getLastAid () =
    let
      val actions = S.tidActions ()
      val (ACTION {aid, ...}) = getActionFromArrayAtIndex (actions, RA.length actions - 1)
    in
      aid
    end


  (********************************************************************
   * Continuation Management
   *******************************************************************)

  fun saveCont f =
  let
    val _ = debug (fn () => "ActionManager.saveCont")
  in
    S.saveCont (f)
  end

  fun restoreCont () =
  let
    val _ = debug (fn () => "ActionManager.restoreCont")
  in
    S.restoreCont ()
  end
end

(* TODO: remove nodes from property list *)
(* TODO: never match communications from the same thread *)
