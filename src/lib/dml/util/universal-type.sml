(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure UniversalType :> UNIVERSAL_TYPE =
   struct
      type t = exn

      fun 'a embed () =
         let
            exception E of 'a
            fun project (e: t): 'a option =
               case e of
                  E a => SOME a
                | _ => NONE
         in
            (E, project)
         end
   end
