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

  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)


  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (********************************************************************
   * Datatypes
   *******************************************************************)

  datatype node = NODE of {array: exn ResizableArray.t, index: int}
  exception NodeExn of (action * (unit -> unit) option)

  fun getActionFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn (act, _) => act
       | _ => raise Fail "getActionFromArrayAtIndex"

  fun updateActionArray (array, index, act) =
  let
    val _ =  case RA.sub (array, index) of
                 NodeExn (_, SOME f) => f ()
               | _ => ()
  in
    RA.update (array, index, NodeExn (act, NONE))
  end

  fun doOnUpdateLastNode wakeup =
  let
    val _ = Assert.assertAtomic' ("PoHelper.doOnUpdateLastNode", NONE)
    val array = S.tidActions ()
    val lastIndex = RA.length array - 1
    val act = getActionFromArrayAtIndex (array, lastIndex)
  in
    RA.update (array, lastIndex, NodeExn (act, SOME wakeup))
  end

  fun addToActionsEnd (array, act) =
    RA.addToEnd (array, NodeExn (act, NONE))


  (********************************************************************
   * Arbitrator interfacing
   *******************************************************************)

  fun sendToArbitrator (NODE {array, index}) =
  let
    val prevAction =
      if index = 0 then NONE
      else SOME (getActionFromArrayAtIndex (array, index - 1))
    val action = getActionFromArrayAtIndex (array, index)
    val _ = Arbitrator.processAdd {action = action, prevAction = prevAction}
  in
    msgSendSafe (AR_REQ_ADD {action = action, prevAction = prevAction})
  end

  fun getFinalAction () =
  let
    val actions = S.tidActions ()
    val finalAction = getActionFromArrayAtIndex (actions, (RA.length actions) - 1)
  in
    finalAction
  end

  (********************************************************************
   * Node management
   *******************************************************************)


  fun getPrevNode (NODE {array, index}) =
    NODE {array = array, index = index - 1}

  fun handleInit {parentAid : action_id} =
  let
    val _ = S.atomicBegin ()
    val actions = S.tidActions ()
    val beginAid = newAid ()
    val act = ACTION {aid = beginAid, act = BEGIN {parentAid = parentAid}}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    (* initial action can be immediately added arbitrator since it will be
    * immediately added to finalSatedComm using forceAddSatedComm (See
    * dml-decentralized.sml where call to handleInit is made.) *)
    val _ = sendToArbitrator node
    val _ = S.atomicEnd ()
  in
    beginAid
  end

  fun insertCommitNode () =
  let
    val _ = S.atomicBegin ()
    val actions = S.tidActions ()
    val comAid = newAid ()
    val act = ACTION {aid = comAid, act = COM}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    (* initial action can be immediately added arbitrator since it will be
    * immediately added to finalSatedComm using forceAddSatedComm (See
    * dml-decentralized.sml where call to insertCommitNode is made.) *)
    val _ = sendToArbitrator node
    val _ = S.atomicEnd ()
  in
    comAid
  end

  fun insertRollbackNode () =
  let
    val actions = S.tidActions ()
    val rbAid = newAid ()
    val act = ACTION {aid = rbAid, act = RB}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    (* initial action can be immediately added arbitrator since it will be
    * immediately added to finalSatedrbm using forceAddSatedrbm (See
    * dml-decentralized.sml where call to insertRollbackNode is made.) *)
    val _ = sendToArbitrator node
  in
    rbAid
  end

  fun handleSpawn {childTid} =
  let
    val actions = S.tidActions ()
    val spawnAid = newAid ()
    val spawnAct = ACTION {aid = spawnAid, act = SPAWN {childTid = childTid}}
    val _ = addToActionsEnd (actions, spawnAct)
    val spawnNode = NODE {array = actions, index = RA.length actions - 1}
    val _ = sendToArbitrator spawnNode
  in
    {spawnAid = spawnAid, spawnNode = spawnNode}
  end

  fun handleSend {cid: channel_id} =
  let
    val actions = S.tidActions ()
    (* act *)
    val actAid = newAid ()
    val actAct = ACTION {aid = actAid, act = SEND_ACT {cid = cid}}
    val _ = addToActionsEnd (actions, actAct)
    (* wait *)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = SEND_WAIT {cid = cid, matchAid = NONE}}
    val _ = addToActionsEnd (actions, waitAct)
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
    val _ = addToActionsEnd (actions, actAct)
    (* wait *)
    val waitAid = newAid ()
    val waitAct = ACTION {aid = waitAid, act = RECV_WAIT {cid = cid, matchAid = NONE}}
    val _ = addToActionsEnd (actions, waitAct)
    val waitNode = NODE {array = actions, index = (RA.length actions) - 1}
  in
    {waitNode = waitNode, actAid = actAid}
  end


  fun setMatchAid (n as NODE {array, index}) (matchAid: action_id) =
  let
    val (ACTION {aid, act}) = getActionFromArrayAtIndex (array, index)
    val newAct = case act of
                      SEND_WAIT {cid, matchAid = NONE} => SEND_WAIT {cid = cid, matchAid = SOME matchAid}
                    | RECV_WAIT {cid, matchAid = NONE} => RECV_WAIT {cid = cid, matchAid = SOME matchAid}
                    | _ => raise Fail "ActionManager.setMatchAid"
    val _ = updateActionArray (array, index, ACTION {aid = aid, act = newAct})
    val _ = sendToArbitrator (getPrevNode n)
    val _ = sendToArbitrator n
  in
    ()
  end

  fun inNonSpecExecMode () =
  let
    val actions = S.tidActions ()
  in
    case getActionFromArrayAtIndex (actions, 0) of
         ACTION {act = COM, ...} => false
       | ACTION {act = BEGIN _, ...} => false
       | ACTION {act = RB, ...} => true
       | _ => raise Fail "POHelper.inNonSpecExecMode: first action is not BEGIN, COM or RB"
  end

  fun isLastNodeMatched () =
  let
    val actions = S.tidActions ()
    val lastIndex = RA.length actions - 1
    val ACTION {act, ...} = getActionFromArrayAtIndex (actions, lastIndex)
  in
    case act of
      SEND_WAIT {matchAid = NONE, ...} => false
    | RECV_WAIT {matchAid = NONE, ...} => false
    | _ => true
  end

  fun isLastAidOnThread (t, aid) =
  let
    val tid = S.getThreadId t
    val actions = CML.tidToActions tid
    val lastIndex = RA.length actions - 1
    val ACTION {aid = lastAid, ...} = getActionFromArrayAtIndex (actions, lastIndex)
  in
    MLton.equal (aid, lastAid)
  end

  fun isLastNode (NODE{array, index}) =
    (debug' ("isLastNode: arrayLength="^(Int.toString (RA.length array))^" index="^(Int.toString index));
     RA.length array - 1 = index)

  fun nodeToAction (NODE{array, index}) = getActionFromArrayAtIndex (array, index)

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
  end handle CML.Kill => S.switchToNext (fn _ => ())
end
