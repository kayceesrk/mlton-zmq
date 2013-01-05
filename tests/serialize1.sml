(* Copyright (C) 2010 Matthew Fluet.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

val v = 0
val ser_v = MLton.serialize v
val v' = MLton.deserialize ser_v
val _ = print ((Int.toString v') ^ "\n")
