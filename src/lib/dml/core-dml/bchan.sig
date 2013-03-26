(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

 signature BCHAN =
 sig
   type 'a bchan

   val newBChan : int -> 'a bchan
   val bsend : 'a bchan * 'a * int -> unit
   val brecv : 'a bchan * int -> 'a
 end
