
structure CommunicationManager : COMM_MANAGER =
struct
  open RepTypes
  open ActionManager
  structure S = CML.Scheduler

  (********************************************************************
   * Debug
   *******************************************************************)

  structure Assert = LocalAssert(val assert = false)
  structure Debug = LocalDebug(val debug = true)


  fun debug msg = Debug.sayDebug ([S.atomicMsg, S.tidMsg], msg)
  fun debug' msg = debug (fn () => msg)

  (********************************************************************
   * Main
   *******************************************************************)

  fun msgToString msg =
    case msg of
         S_ACT  {channel = ChannelId cidStr, sendActAid, ...} =>
           concat ["S_ACT[", cidStr, ",", aidToString sendActAid, "]"]
       | R_ACT  {channel = ChannelId cidStr, recvActAid} =>
           concat ["R_ACT[", cidStr, ",", aidToString recvActAid, "]"]
       | S_JOIN {channel = ChannelId cidStr, sendActAid, recvActAid} =>
           concat ["S_JOIN[", cidStr, ",", aidToString sendActAid, ",", aidToString recvActAid, "]"]
       | R_JOIN {channel = ChannelId cidStr, recvActAid, sendActAid} =>
           concat ["R_JOIN[", cidStr, ",", aidToString recvActAid, ",", aidToString sendActAid, "]"]
       | CONN   {pid = ProcessId pidInt} =>
           concat ["CONN[", Int.toString pidInt, "]"]
       | SATED  {recipient = ProcessId pidInt, remoteAid, matchAid} =>
           concat ["SATED[",Int.toString pidInt, ",", aidToString remoteAid, ",", aidToString matchAid, "]"]
       | AR_REQ_ADD {action, prevAction} =>
           concat ["AR_REQ_ADD[", actionToString action, ",",
                   case prevAction of NONE => "NONE" | SOME a => actionToString a, "]"]
       | AR_REQ_COM {action} => concat ["AR_REQ_COM[", actionToString action, "]"]


  fun msgSend (msg : msg) =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.msgSend", SOME 1)
    val PROXY {sink, ...} = !proxy
    val _ = ZMQ.send (valOf sink, ROOTED_MSG {msg = msg, sender = ProcessId (!processId)})
    val _ = debug (fn () => "Sent: "^(msgToString msg))
  in
    ()
  end

  fun msgSendSafe msg =
  let
    val _ = Assert.assertNonAtomic' ("DmlDecentralized.msgSendSafe")
    val _ = S.atomicBegin ()
    val _ = msgSend msg
    val _ = S.atomicEnd ()
  in
    ()
  end

  fun msgRecv () : msg option =
  let
    val _ = Assert.assertAtomic' ("DmlDecentralized.msgRecv", SOME 1)
    val PROXY {source, ...} = !proxy
  in
    case ZMQ.recvNB (valOf source) of
         NONE => NONE
       | SOME (ROOTED_MSG {msg, sender = ProcessId pidInt}) =>
           if pidInt = (!processId) then NONE
           else
             let
               val _ = debug (fn () => "Received: "^(msgToString msg))
             in
              SOME msg
             end
  end

  fun msgRecvSafe () =
  let
    val _ = Assert.assertNonAtomic' ("DmlDecentralized.msgRecvSafe")
    val _ = S.atomicBegin ()
    val r = msgRecv ()
    val _ = S.atomicEnd ()
  in
    r
  end

end
