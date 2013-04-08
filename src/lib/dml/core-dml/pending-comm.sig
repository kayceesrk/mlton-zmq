signature PENDING_COMM =
sig
  type 'a t
  val empty     : unit -> 'a t
  val addAid    : 'a t -> RepTypes.channel_id -> ActionHelper.action_id -> 'a -> unit
  val removeAid : 'a t -> RepTypes.channel_id -> ActionHelper.action_id -> unit
  val deque     : 'a t -> RepTypes.channel_id -> {againstAid: ActionHelper.action_id}
                   -> (ActionHelper.action_id * 'a) option
  val cleanup   : 'a t -> int RepTypes.PTRDict.dict -> unit
end
