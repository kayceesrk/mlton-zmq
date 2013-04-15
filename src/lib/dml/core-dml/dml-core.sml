(* Copyright (C) 2013 KC Sivaramakrishnan.
:q
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure DmlCore : DML_INTERNAL =
struct
  structure Assert = LocalAssert(val assert = true)
  structure Debug = LocalDebug(val debug = true)

  open RepTypes
  open ActionHelper
  structure ZMQ = MLton.ZMQ
  structure S = CML.Scheduler
  structure O = Orchestrator
  structure GM = GraphManager
  structure C = CML
  structure CD = CycleDetector


  datatype z1 = datatype GraphManager.comm_result
  datatype z2 = datatype Orchestrator.caller_kind
  datatype chan = datatype RepTypes.chan

  (* -------------------------------------------------------------------- *)
  (* Debug helper functions *)
  (* -------------------------------------------------------------------- *)

  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (* -------------------------------------------------------------------- *)

  fun runDML (f, to) =
    let
      val _ = Assert.assert ([], fn () => "runDML must be run after connect",
                             fn () => case !proxy of
                                        PROXY {sink = NONE, ...} => false
                                      | _ => true)
      fun body () =
      let
        (* start the daemon. Insert a CR_RB node just so that functions which
         * expect a node at every tid will not throw exceptions. *)
        val _ = C.spawn (fn () =>
                  let
                    val _ = GM.insertCommitNode ()
                  in
                    O.clientDaemon ()
                  end)
        val _ = GM.insertCommitNode ()
        val _ = O.saveCont ()
        val _ = f ()
      in
        ()
      end

      val _ = RunCML.doit (body, to)
      val PROXY {source, sink, ...} = !proxy
      val _ = ZMQ.sockClose (valOf source)
      val _ = ZMQ.sockClose (valOf sink)
    in
      OS.Process.success
    end

  fun channel s = CHANNEL (ChannelId s)

  fun send (CHANNEL c, m) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlCore.send(1)")
  in
    case GM.handleSend {cid = c} of
      UNCACHED {actAid, waitNode} =>
        let
          val m = MLton.serialize (m)
          val _ = O.processSend {callerKind = Client, channel = c,
            sendActAid = actAid, sendWaitNode = waitNode, value = m}
        in
          if O.inNonSpecExecMode () andalso
              not (GM.isLastNodeMatched ()) then
            let
              val {read = wait, write = wakeup} = IVar.new ()
              val _ = GM.doOnUpdateLastNode wakeup
              val _ = S.atomicEnd ()
            in
              wait ()
            end
          else S.atomicEnd ()
        end
    | CACHED _ => S.atomicEnd ()
  end

  fun recv (CHANNEL c) =
  let
    val _ = S.atomicBegin ()
    val _ = debug' ("DmlCore.recv(1)")
    val nonSpec = true
  in
    case GM.handleRecv {cid = c} of
      UNCACHED {actAid, waitNode} =>
        let
          val serM = O.processRecv {callerKind = Client, channel = c,
            recvActAid = actAid, recvWaitNode = waitNode}
          val _ = if (nonSpec orelse O.inNonSpecExecMode ()) andalso
                    not (GM.isLastNodeMatched ()) then
                    let
                      val {read = wait, write = wakeup} = IVar.new ()
                      val _ = S.doAtomic (fn () => GM.doOnUpdateLastNode wakeup)
                    in
                      wait ()
                    end
                  else ()
          val _ = GM.insertNoopNode ()
          val serM = case GM.getValue waitNode of
                       NONE => serM (* For spec exec *)
                     | SOME s => s
          val result = MLton.deserialize serM
          val _ = debug' ("DmlCore.recv(2.1)")
        in
          result
        end
    | CACHED value =>
         (S.atomicEnd ();
          debug' ("DmlCore.recv(2.2)");
          MLton.deserialize value)
  end

  val exitDaemon = fn () => O.exitDaemon := true

  fun commit () =
  let
    val finalAction = GM.getFinalAction ()
    val {read, write} = IVar.new ()
    val _ = CML.spawn (fn () => CD.processCommit {action = finalAction, pushResult = write})
    val _ = case read () of
                 AR_RES_SUCC _ => ()
               | AR_RES_FAIL {rollbackAids, dfsStartAct} =>
                   let
                     val _ = debug (fn () => "Commit Failure: size(rollbackAids)="^
                                   (Int.toString (PTRDict.size (rollbackAids))))
                     val () = S.atomicBegin ()
                     val () = O.processRollbackMsg rollbackAids dfsStartAct
                     val () = S.atomicEnd ()
                   in
                     O.restoreCont (PTRDict.lookup rollbackAids (aidToPtr (actionToAid finalAction)))
                   end
               | _ => raise Fail "DmlCore.commit: unexpected message"

    (* Committed Successfully *)
    val _ = debug' ("DmlCore.commit: SUCCESS")
    (* --- Racy code --- *)
    val _ = CML.tidCommit (S.getCurThreadId ())
    (* --- End racy code --- *)
    val _ = GM.insertCommitNode ()
    val _ = O.saveCont ()
    val _ = Assert.assertNonAtomic' ("DmlCore.commit")
  in
    ()
  end

  val _ = SchedulerHelper.commitRef := commit

  fun spawn f =
    let
      val _ = commit ()
      val tid = S.newTid ()
      val tidInt = C.tidToInt tid
      val {spawnAid, spawnNode = _}= GM.handleSpawn {childTid = ThreadId tidInt}
      fun prolog () =
        let
          val _ = GM.handleInit {parentAid = SOME spawnAid}
        in
          O.saveCont ()
        end
      val _ = ignore (C.spawnWithTid (f o prolog, tid))
      val _ = commit ()
    in
      ()
    end

  val yield = C.yield

  fun getThreadId () =
  let
    val pidInt = !processId
    val tidInt = S.tidInt ()
  in
    {pid = pidInt, tid = tidInt}
  end

  fun touchLastComm () =
  let
    val _ = S.atomicBegin ()
  in
    if not (GM.isLastNodeMatched ()) then
      let
        val {read = wait, write = wakeup} = IVar.new ()
        val _ = GM.doOnUpdateLastNode wakeup
        val _ = S.atomicEnd ()
      in
        wait ()
      end
    else S.atomicEnd ()
  end


  val connect = O.connect
  val startProxy = O.startProxy
end

