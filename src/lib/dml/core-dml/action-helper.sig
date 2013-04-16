(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)


signature ACTION_HELPER =
sig

  type action_id = RepTypes.action_id
  type action = RepTypes.action
  type ptr = RepTypes.ptr


  val dummyAid    : action_id
  val newAid      : unit -> action_id
  val aidToPidInt : action_id -> int
  val aidToTidInt : action_id -> int
  val aidToRidInt : action_id -> int
  val aidToTid    : action_id -> RepTypes.thread_id
  val aidToString : action_id -> string
  val aidToActNum : action_id -> int
  val isAidLocal  : action_id -> bool
  (* val getPrevAid  : action_id -> action_id *)
  val getNextAid  : action_id -> action_id
  val aidToPtr    : action_id -> ptr
  val ptrToString : ptr -> string

  val actionToAid : action -> action_id
  val actionToString : action -> string
end
