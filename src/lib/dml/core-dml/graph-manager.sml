(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure GraphManager : GRAPH_MANAGER =
struct
  structure S = CML.Scheduler
  structure RA = ResizableArray
  open RepTypes
  open ActionHelper
  open CommunicationHelper

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

  datatype comm_result = UNCACHED of {waitNode: node, actAid: action_id}
                       | CACHED of RepTypes.w8vec

  exception NodeExn of {action: action,
                        callback: (unit -> unit) option,
                        value: w8vec option}

  exception CacheItem of {actNum: int,  (* recvWait or sendWait's action number *)
                          value: w8vec} (* emptyW8Vec for send *)


  fun getActionFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn {action,...} => action
       | _ => raise Fail "getActionFromArrayAtIndex"

  fun getValueFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn {value,...} => value
       | _ => raise Fail "getActionFromArrayAtIndex"

  fun updateActionArray (array, index, act, value) =
  let
    val _ =  case RA.sub (array, index) of
                 NodeExn {callback = SOME f, ...} => f ()
               | _ => ()
  in
    RA.update (array, index, NodeExn {action = act, callback = NONE, value = SOME value})
  end

  fun doOnUpdateLastNode wakeup =
  let
    val _ = Assert.assertAtomic' ("PoHelper.doOnUpdateLastNode", NONE)
    val array = S.tidActions ()
    val lastIndex = RA.length array - 1
    val act = getActionFromArrayAtIndex (array, lastIndex)
    val v = getValueFromArrayAtIndex (array, lastIndex)
  in
    RA.update (array, lastIndex, NodeExn {action = act, callback = SOME wakeup, value = v})
  end

  fun addToActionsEnd (array, act) =
    RA.addToEnd (array, NodeExn {action = act, callback = NONE, value = NONE})


  (********************************************************************
   * CycleDetector interfacing
   *******************************************************************)

  fun sendToCycleDetector (NODE {array, index}) =
  let
    val prevAction =
      if index = 0 then NONE
      else SOME (getActionFromArrayAtIndex (array, index - 1))
    val action = getActionFromArrayAtIndex (array, index)
    val _ = CycleDetector.processAdd {action = action, prevAction = prevAction}
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
    * dml-core.sml where call to handleInit is made.) *)
    val _ = sendToCycleDetector node
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
    * dml-core.sml where call to insertCommitNode is made.) *)
    val _ = sendToCycleDetector node
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
    * dml-core.sml where call to insertRollbackNode is made.) *)
    val _ = sendToCycleDetector node
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
    val _ = sendToCycleDetector spawnNode
  in
    {spawnAid = spawnAid, spawnNode = spawnNode}
  end

  fun setMatchAid (n as NODE {array, index}) (matchAid: action_id) (value: w8vec) =
  let
    val (ACTION {aid, act}) = getActionFromArrayAtIndex (array, index)
    val newAct = case act of
                      SEND_WAIT {cid, matchAid = NONE} => SEND_WAIT {cid = cid, matchAid = SOME matchAid}
                    | RECV_WAIT {cid, matchAid = NONE} => RECV_WAIT {cid = cid, matchAid = SOME matchAid}
                    | _ => raise Fail "ActionHelper.setMatchAid"
    val _ = updateActionArray (array, index, ACTION {aid = aid, act = newAct}, value)
    val _ = sendToCycleDetector (getPrevNode n)
    val _ = sendToCycleDetector n
  in
    ()
  end


  fun handleSend {cid: channel_id} =
  let
    val cache = S.tidCache ()
    fun handleSendUncached {cid: channel_id} =
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
      UNCACHED {waitNode = waitNode, actAid = actAid}
    end

    fun handleSendCached {cid: channel_id} =
    case !cache of
        CacheItem{actNum,value}::ctl =>
        let
          val _ = debug' ("handleSendCached")
          val (waitNode, actAid) = case handleSendUncached {cid = cid} of
              UNCACHED {waitNode, actAid} => (waitNode, actAid)
            | _ => raise Fail "handleSendCached: impossible!"
          val _ = Assert.assert ([], fn () => "handleSendCached: actNum mis-match!",
            fn () => actNum - 1 = aidToActNum actAid)
          val _ = setMatchAid waitNode actAid value
          val _ = cache := ctl
        in
          CACHED value
        end
      | _ => raise Fail "handleSendCached: empty"
  in
    if length (!cache) > 0 then
      handleSendCached {cid = cid}
    else
      handleSendUncached {cid = cid}
  end

  fun handleRecv {cid: channel_id} =
  let
    val cache = S.tidCache ()
    fun handleRecvUncached {cid: channel_id} =
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
      UNCACHED {waitNode = waitNode, actAid = actAid}
    end

    fun handleRecvCached {cid: channel_id} =
    case !cache of
          CacheItem{actNum,value}::ctl =>
          let
            val _ = debug' ("handleRecvCached")
            val (waitNode, actAid) = case handleRecvUncached {cid = cid} of
                UNCACHED {waitNode, actAid} => (waitNode, actAid)
              | _ => raise Fail "handleRecvCached: impossible!"
            val _ = Assert.assert ([], fn () => "handleRecvCached: actNum mis-match!",
            fn () => actNum - 1 = aidToActNum actAid)
            val _ = setMatchAid waitNode actAid value
            val _ = cache := ctl
          in
            CACHED value
          end
        | _ => raise Fail "handleRecvCached: empty"
  in
    if length (!cache) > 0 then
      handleRecvCached {cid = cid}
    else
      handleRecvUncached {cid = cid}
  end

  fun inNonSpecExecMode () =
  let
    val actions = S.tidActions ()
  in
    case getActionFromArrayAtIndex (actions, 0) of
         ACTION {act = COM, ...} => false
       | ACTION {act = BEGIN _, ...} => false
       | ACTION {act = RB, ...} => true
       | _ => raise Fail "GraphManager.inNonSpecExecMode: first action is not BEGIN, COM or RB"
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
    ActionIdOrdered.eq (aid, lastAid)
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
    val _ = debug (fn () => "ActionHelper.saveCont")
  in
    S.saveCont (f)
  end

  fun restoreCont actionNum =
  let
    val _ = debug (fn () => "ActionHelper.restoreCont")
    val actions = CML.tidToActions (S.getCurThreadId ())

    fun getCacheItem anum idx =
      CacheItem {actNum = anum, value = valOf (getValueFromArrayAtIndex (actions, idx))}

    val ACTION {aid, ...} = getActionFromArrayAtIndex (actions, 0)
    val anumOfFirstAction = aidToActNum aid

    fun loop idx acc =
      if (idx + anumOfFirstAction) < actionNum then
        case getActionFromArrayAtIndex (actions, idx) of
          ACTION {aid, act = SEND_WAIT _} => loop (idx+1) ((getCacheItem (aidToActNum aid) idx)::acc)
        | ACTION {aid, act = RECV_WAIT _} => loop (idx+1) ((getCacheItem (aidToActNum aid) idx)::acc)
        | _ => loop (idx+1) acc
      else acc

    val cache = rev (loop 0 [])
    val _ = debug (fn () => "ActionHelper.restoreCont: cacheLength="^(Int.toString (length cache)))
  in
    S.restoreCont cache
  end handle CML.Kill => S.switchToNext (fn _ => ())
end
