(* Copyright (C) 2010 Matthew Fluet.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

val r = ref 1
val ser_r = MLton.serialize r
val _ = r := 2
val r' : int ref = MLton.deserialize ser_r

(* r' is still 1 since serialize copies mutable objects at the point of
 * serialization *)
val _ = print (Int.toString (!r'))
