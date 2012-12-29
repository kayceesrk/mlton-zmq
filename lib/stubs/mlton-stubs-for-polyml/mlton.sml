(* Copyright (C) 2009 Matthew Fluet.
 * Copyright (C) 2004-2005 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure MLton =
   struct
      val isMLton = false
      val size : 'a -> int = PolyML.objSize
      structure Exn =
         struct
            val history : exn -> string list = fn _ => []
         end
      structure GC =
         struct
            fun collect () = PolyML.fullGC ()
            fun setMessages (b : bool) = ()
            fun pack () = collect ()
         end
      structure Platform =
         struct
            local
               fun mkHost cmd =
                  let
                     fun findCmd dir =
                        let
                           val cmd = dir ^ "/bin/" ^ cmd
                           val upDir = OS.FileSys.realPath (dir ^ "/..")
                        in
                           if OS.FileSys.access (cmd, [OS.FileSys.A_EXEC])
                              then SOME cmd
                           else if dir <> upDir
                              then findCmd upDir
                           else NONE
                        end
                     val proc = Unix.execute (valOf (findCmd "."), [])
                     val ins = Unix.textInstreamOf proc
                     val hostString = TextIO.inputAll ins
                     val status = Unix.reap proc
                  in
                     String.extract
                     (hostString, 0, SOME (String.size hostString - 1))
                  end
            in
               structure Arch =
                  struct
                     type t = string
                     val toString = fn s => s
                     val host = mkHost "host-arch"
                  end
               structure OS =
                  struct
                     type t = string
                     val toString = fn s => s
                     val host = mkHost "host-os"
                  end
            end
         end
   end
