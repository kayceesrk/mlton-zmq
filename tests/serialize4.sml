(* Copyright (C) 2010 Matthew Fluet.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)


(* This test --fails-- *)
val f = print
val ser_f = MLton.serialize f
val f' : string -> unit = MLton.deserialize ser_f
val _ = f' (Int.toString 0)
