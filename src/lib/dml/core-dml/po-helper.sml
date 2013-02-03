(* action-manager.sml
 *
 * 2013 KC Sivaramakrishnan
 *
 * Action helper
 *
 *)


structure POHelper : PO_HELPER =
struct
  structure S = CML.Scheduler
  structure RA = ResizableArray
  open RepTypes
  open ActionManager
  open CommunicationManager

  (********************************************************************
   * Debug
   *******************************************************************)

  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)


  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (********************************************************************
   * Datatypes
   *******************************************************************)

  datatype node = NODE of {array: exn ResizableArray.t, index: int}
  exception NodeExn of action

  fun getActionFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn act => act
       | _ => raise Fail "getActionFromArrayAtIndex"


  (********************************************************************
   * Arbitrator interfacing
   *******************************************************************)

  fun handleFinalizingSatedNode (NODE {array, index}) =
  let
    val prevAction =
      if index = 0 then NONE
      else SOME (getActionFromArrayAtIndex (array, index - 1))
  in
    msgSendSafe (AR_REQ_ADD {action = getActionFromArrayAtIndex (array, index),
                         prevAction = prevAction})
  end

  fun requestCommit () =
  let
    val _ = Assert.assertAtomic' ("POHelper.requestCommit", SOME 1)
    val actions = S.tidActions ()
    val lastAction = getActionFromArrayAtIndex (actions, (RA.length actions) - 1)
  in
    msgSend (AR_REQ_COM {action = lastAction})
  end

  (********************************************************************
   * Node management
   *******************************************************************)


  fun getPrevNode (NODE {array, index}) =
    NODE {array = array, index = index - 1}

  fun handleInit {parentAid : action_id} =
  let
    val actions = S.tidActions ()
    val beginAid = newAid ()
    val act = ACTION {aid = beginAid, act = BEGIN {parentAid = parentAid}}
    val _ = RA.addToEnd (actions, NodeExn act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    (* initial action can be immediately added arbitrator since it will be
    * immediately added to finalSatedComm using forceAddSatedComm (See
    * dml-decentralized.sml where call to handleInit is made.) *)
    val _ = handleFinalizingSatedNode node
  in
    beginAid
  end

  fun insertCommitRollbackNode () =
  let
    val actions = S.tidActions ()
    val comRbAid = newAid ()
    val act = ACTION {aid = comRbAid, act = COM_RB}
    val _ = RA.addToEnd (actions, NodeExn act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    (* initial action can be immediately added arbitrator since it will be
    * immediately added to finalSatedComm using forceAddSatedComm (See
    * dml-decentralized.sml where call to insertCommitRollbackNode is made.) *)
    val _ = handleFinalizingSatedNode node
  in
    comRbAid
  end

  fun handleSpawn {childTid} =
  let
    val actions = S.tidActions ()
    val spawnAid = newAid ()
    val spawnAct = ACTION {aid = spawnAid, act = SPAWN {childTid = childTid}}
    val _ = RA.addToEnd (actions, NodeExn spawnAct)
    val spawnNode = NODE {array = actions, index = RA.length actions - 1}
  in
    {spawnAid = spawnAid, spawnNode = spawnNode}
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
