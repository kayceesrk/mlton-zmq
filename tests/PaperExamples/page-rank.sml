(* Copyright (C) 2013 KC Sivaramakrishnan.
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

open Dml

structure PageRank =
struct

  type page_rank = real

  fun computePageRank {initRank: page_rank,
                       inEdgeChanList: page_rank chan list,
                       outEdgeChanList: page_rank chan list} =

end
