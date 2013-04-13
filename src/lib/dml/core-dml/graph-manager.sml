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
                        callback: (unit -> unit),
                        value: w8vec option}

  exception CacheItem of {actNum: int,  (* recvWait or sendWait's action number *)
                          value: w8vec} (* emptyW8Vec for send *)


  fun getActionFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn {action,...} => action
       | _ => raise Fail "NodeExn"

  fun getValueFromArrayAtIndex (array, index) =
    case RA.sub (array, index) of
         NodeExn {value,...} => value
       | _ => raise Fail "NodeExn"

  fun updateActionArray (array, index, act, value) =
  let
    val callback =  case RA.sub (array, index) of
                 NodeExn {callback = f, ...} => f
               | _ => (fn () => ())
    val _ = RA.update (array, index,
      NodeExn {action = act, callback = fn () => (), value = SOME value})
  in
    callback ()
  end

  fun doOnUpdateNode (NODE{array, index}) wakeup =
  let
    val _ = Assert.assertAtomic' ("PoHelper.doOnUpdateLastNode", NONE)
    val {action, value, callback} =
      case RA.sub (array, index) of
           NodeExn m => m
         | _ => raise Fail "NodeExn"
  in
    RA.update (array, index, NodeExn {action = action, callback = wakeup o callback, value = value})
  end

  fun doOnUpdateLastNode wakeup =
  let
    val array = S.tidActions ()
    val index = RA.length array - 1
  in
    doOnUpdateNode (NODE {array = array, index = index}) wakeup
  end

  fun addToActionsEnd (array, act) =
    RA.addToEnd (array, NodeExn {action = act, callback = fn () => (), value = NONE})


  (********************************************************************
   * CycleDetector interfacing
   *******************************************************************)

  fun sendToCycleDetector (NODE {array, index}) =
  let
    val _ = Assert.assert ([], fn () => "GraphManager.sendToCycleDetector",
            fn () => case getActionFromArrayAtIndex (array, index) of
                   BASE _ => true
                 | EVENT _ => false)
    fun sendCore prevAction =
    let
      val action = getActionFromArrayAtIndex (array, index)
      val _ = CycleDetector.processAdd {action = action, prevAction = prevAction}
    in
      msgSendSafe (AR_REQ_ADD {action = action, prevAction = prevAction})
    end

  in
    if index = 0 then sendCore NONE
    else (case getActionFromArrayAtIndex (array, index - 1) of
               BASE m => sendCore (SOME (BASE m))
             | EVENT _ =>
                 let
                   val node = NODE {array = array, index = index}
                 in
                  doOnUpdateNode node (fn () => sendToCycleDetector node)
                 end)
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
    val act = BASE {aid = beginAid, act = BEGIN {parentAid = parentAid}}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    val _ = sendToCycleDetector node
    val _ = S.atomicEnd ()
  in
    beginAid
  end

  fun insertNoopNode () =
  let
    val _ = S.atomicBegin ()
    val actions = S.tidActions ()
    val noopAid = newAid ()
    val act = BASE {aid = noopAid, act = NOOP}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    val _ = sendToCycleDetector node
    val _ = S.atomicEnd ()
  in
    noopAid
  end

  fun insertCommitNode () =
  let
    val _ = S.atomicBegin ()
    val actions = S.tidActions ()
    val comAid = newAid ()
    val act = BASE {aid = comAid, act = COM}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    val _ = sendToCycleDetector node
    val _ = S.atomicEnd ()
  in
    comAid
  end

  fun insertRollbackNode () =
  let
    val actions = S.tidActions ()
    val rbAid = newAid ()
    val act = BASE {aid = rbAid, act = RB}
    val _ = addToActionsEnd (actions, act)
    val node = NODE {array = actions, index = RA.length actions - 1}
    val _ = sendToCycleDetector node
  in
    rbAid
  end

  fun handleSpawn {childTid} =
  let
    val actions = S.tidActions ()
    val spawnAid = newAid ()
    val spawnAct = BASE {aid = spawnAid, act = SPAWN {childTid = childTid}}
    val _ = addToActionsEnd (actions, spawnAct)
    val spawnNode = NODE {array = actions, index = RA.length actions - 1}
    val _ = sendToCycleDetector spawnNode
  in
    {spawnAid = spawnAid, spawnNode = spawnNode}
  end

  fun setMatchAidSimple (n as NODE {array, index}) (matchAid: action_id) (value: w8vec) =
  let
    val {aid, act} = case getActionFromArrayAtIndex (array, index) of
                        BASE m => m
                      | _ => raise Fail "setMatAidSimple: unexpected!"
    val newAct = case act of
                      SEND_WAIT {cid, matchAid = NONE} => SEND_WAIT {cid = cid, matchAid = SOME matchAid}
                    | RECV_WAIT {cid, matchAid = NONE} => RECV_WAIT {cid = cid, matchAid = SOME matchAid}
                    | _ => raise Fail ("GraphManager.setMatchAid: Action="^
                                       (actionToString (BASE {aid=aid,act=act})))
    val _ = updateActionArray (array, index, BASE {aid = aid, act = newAct}, value)
    val _ = sendToCycleDetector (getPrevNode n)
    val _ = sendToCycleDetector n
  in
    ()
  end

  fun setMatchAid {waitNode as NODE{array, index}, actAid, matchAid, value} =
    (Assert.assertAtomic' ("setMatchAid", NONE);
     case getActionFromArrayAtIndex (array, index) of
          BASE _ => setMatchAidSimple waitNode matchAid value
        | EVENT {actions = axns} =>
            let
              (* helper function to replace EVENT with BASE in a node *)
              fun updateNode (NODE {array, index}) aid =
              let
                val (action, callback, value) =
                  case RA.sub (array, index) of
                       NodeExn {action, callback, value} => (action, callback, value)
                     | _ => raise Fail "NodeExn"
                val actions = case action of
                                   EVENT {actions, ...} => actions
                                 | _ => raise Fail "setMatchAid: unexpected"
                val act = AidDict.lookup actions aid
                val newNode = NodeExn {action = BASE {aid = aid, act = act}, callback = callback, value = value}
              in
                RA.update (array, index, newNode)
              end

              (* update act node *)
              val actNode = NODE{array = array, index = index - 1}
              val _ = updateNode actNode actAid
              (* update wait node *)
              val waitAid = actNumPlus actAid (AidDict.size axns)
              val _ = updateNode waitNode waitAid

              val _ = setMatchAidSimple waitNode matchAid value
              (* send out clean message *)
              val axns = AidDict.remove axns actAid
              val _ = msgSend (CLEAN {actions = axns})
            in
              ()
            end)

  fun handleSend {cid: channel_id} =
  let
    val cache = S.tidCache ()
    fun handleSendUncached {cid: channel_id} =
    let
      val actions = S.tidActions ()
      (* act *)
      val actAid = newAid ()
      val actAct = BASE {aid = actAid, act = SEND_ACT {cid = cid}}
      val _ = addToActionsEnd (actions, actAct)
      (* wait *)
      val waitAid = newAid ()
      val waitAct = BASE {aid = waitAid, act = SEND_WAIT {cid = cid, matchAid = NONE}}
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
          val _ = setMatchAidSimple waitNode actAid value
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
      val actAct = BASE {aid = actAid, act = RECV_ACT {cid = cid}}
      val _ = addToActionsEnd (actions, actAct)
      (* wait *)
      val waitAid = newAid ()
      val waitAct = BASE {aid = waitAid, act = RECV_WAIT {cid = cid, matchAid = NONE}}
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
            val _ = setMatchAidSimple waitNode actAid value
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
         BASE {act = COM, ...} => false
       | BASE {act = BEGIN _, ...} => false
       | BASE {act = RB, ...} => true
       | _ => raise Fail "GraphManager.inNonSpecExecMode: first action is not BEGIN, COM or RB"
  end

  exception RET_FALSE

  fun isLastNodeMatched () =
  let
    val actions = S.tidActions ()
    val lastIndex = RA.length actions - 1
    val act =
      case getActionFromArrayAtIndex (actions, lastIndex) of
           EVENT _ => raise RET_FALSE
         | BASE {act, ...} => act
  in
    case act of
      SEND_WAIT {matchAid = NONE, ...} => false
    | RECV_WAIT {matchAid = NONE, ...} => false
    | _ => true
  end handle RET_FALSE => false

  fun isLastNode (NODE{array, index}) =
    (debug' ("isLastNode: arrayLength="^(Int.toString (RA.length array))^" index="^(Int.toString index));
     RA.length array - 1 = index)

  fun nodeToAction (NODE{array, index}) = getActionFromArrayAtIndex (array, index)

  fun getValue (NODE{array,index}) =
    getValueFromArrayAtIndex (array, index)

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

    val aid = case getActionFromArrayAtIndex (actions, 0) of
                BASE {aid, ...} => aid
              | _ => raise Fail "GraphManager.restoreCont: first action is a choice?"
    val anumOfFirstAction = aidToActNum aid

    fun loop idx acc =
      if (idx + anumOfFirstAction) < actionNum then
        case getActionFromArrayAtIndex (actions, idx) of
          BASE {aid, act = SEND_WAIT _} => loop (idx+1) ((getCacheItem (aidToActNum aid) idx)::acc)
        | BASE {aid, act = RECV_WAIT _} => loop (idx+1) ((getCacheItem (aidToActNum aid) idx)::acc)
        | _ => loop (idx+1) acc
      else acc

    val cache = rev (loop 0 [])
    val _ = debug (fn () => "ActionHelper.restoreCont: cacheLength="^(Int.toString (length cache)))
  in
    S.restoreCont cache
  end handle CML.Kill => S.switchToNext (fn _ => ())

  fun getWaitAid {actAid, waitNode = NODE {array, index}} =
    case getActionFromArrayAtIndex (array, index) of
         BASE _ => getNextAid actAid
       | EVENT {actions, ...} => actNumPlus actAid (AidDict.size actions)

end
