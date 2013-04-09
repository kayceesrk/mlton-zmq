(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature CYCLE_DETECTOR =
sig
  val processAdd    : {action: RepTypes.action, prevAction: RepTypes.action option} -> unit
  val processCommit : {action: RepTypes.action, pushResult: RepTypes.msg -> unit} -> unit
  val isMatched     : RepTypes.action_id -> bool
end
