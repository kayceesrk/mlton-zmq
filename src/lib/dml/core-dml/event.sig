(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature EVENT =
sig
  type 'a event

  val sendEvt : 'a RepTypes.chan * 'a -> unit event
  val recvEvt : 'a RepTypes.chan -> 'a event
  val wrap    : 'a event -> ('a -> 'b) -> 'b event
  val choose  : 'a event list -> 'a event
  val sync    : 'a event -> 'a
end

signature EVENT_INTERNAL =
sig
  include EVENT
end
