(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature MATCHED_COMM =
sig
  type 'a t
  datatype 'a join_result =
    SUCCESS of {value : 'a, waitNode: GraphManager.node}
  | FAILURE of {actAid : ActionHelper.action_id,
                waitNode : GraphManager.node,
                value : 'a}
  | NOOP

  val empty : unit -> 'a t
  val add   : 'a t -> {channel: RepTypes.channel_id,
                       actAid: ActionHelper.action_id,
                       remoteMatchAid: ActionHelper.action_id,
                       waitNode: GraphManager.node} -> 'a -> unit
  val join  : 'a t -> {remoteAid: ActionHelper.action_id,
                       ignoreFailure: bool,
                       withAid: ActionHelper.action_id} -> 'a join_result
  val cleanup : 'a t -> int RepTypes.PTRDict.dict ->
                {channel: RepTypes.channel_id, actAid: ActionHelper.action_id,
                 waitNode: GraphManager.node, value: 'a} list
  val contains : 'a t -> ActionHelper.action_id -> bool
end
