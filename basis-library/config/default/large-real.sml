(* Copyright (C) 1999-2006 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure LargeReal = Real64

functor LargeReal_ChooseRealN (A: CHOOSE_REALN_ARG) :
   sig val f : LargeReal.real A.t end =
   ChooseRealN_Real64 (A)
