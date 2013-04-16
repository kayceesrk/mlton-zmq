(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature PENDING_COMM =
sig
  type 'a t
  val empty     : unit -> 'a t
  val contains  : 'a t -> RepTypes.channel_id -> ActionHelper.action_id -> bool
  val addAid    : 'a t -> RepTypes.channel_id -> ActionHelper.action_id -> 'a -> unit
  val removeAid : 'a t -> RepTypes.channel_id -> ActionHelper.action_id -> 'a option
  val deque     : 'a t -> RepTypes.channel_id -> {againstAid: ActionHelper.action_id}
                   -> (ActionHelper.action_id * 'a) option
  val cleanup   : 'a t -> int RepTypes.PTRDict.dict -> unit
end
