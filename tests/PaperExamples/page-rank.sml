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
                       outEdgeChanList: page_rank chan list,
                       numIterations: int} =
  let
    val {pid, tid} = getThreadId ()

    fun processor iter waitChan =
    let
      val proceedChan = channel ("PR_CHAN_"^(Int.toString pid)^"_"^(Int.toString tid)^(Int.toString iter))
      fun core () =
      let
        val _ = recv waitChan
        val inputs = List.map (fn c => recv c) inEdgeChanList
        val pr = List.foldl (fn (pr, acc) => pr + acc) 0.0 inputs
        val pr = Real./ (pr, Real.fromInt (List.length outEdgeChanList))
        val _ = List.map (fn c => send (c, pr)) outEdgeChanList
        val _ = send (proceedChan, pr)
      in
        ()
      end
      val _ = spawn core
    in
      proceedChan
    end

    val proceedChan = channel ("PR_CHAN_"^(Int.toString pid)^"_"^(Int.toString tid)^"_Init")
    val _ = spawn (fn () => send (proceedChan, initRank))

    fun driver iter proceedChan =
      if iter = 0 then recv proceedChan
      else
        driver (iter-1) (processor iter proceedChan)
  in
    driver numIterations proceedChan
  end

end
