(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

(* Functions can also be serialized. *)
val f = fn x => x+1
val ser_f = MLton.serialize f
val f' : (int -> int) = MLton.deserialize ser_f
val _ = print (Int.toString (f' 1))
