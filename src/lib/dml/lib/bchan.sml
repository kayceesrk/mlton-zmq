(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure BChan : BCHAN =
struct
  open Dml

  (* BCHAN of (value chan list * ack chan list) *)
  datatype 'a bchan = BCHAN of ('a chan list * unit chan list)

  fun newBChan (n, s) =
    BCHAN (List.tabulate(n, fn _ => channel (s^"_V_"^(Int.toString n))),
           List.tabulate(n, fn _ => channel (s^"_A_"^(Int.toString n))))

  fun bsend (BCHAN (vcList, acList), v, id) =
  let
    val my_vc = List.nth (vcList, id)
    val _ = map (fn vc =>
      if (MLton.equal (vc,my_vc)) then ()
      else send(vc,v)) vcList
    val my_ac = List.nth (acList, id)
    val _ = map (fn ac =>
      if (MLton.equal (ac, my_ac)) then ()
      else recv ac) acList
  in
    ()
  end

  fun brecv (BCHAN (vcList, acList), id) =
  let
    val v = recv (List.nth (vcList, id))
    val _ = send (List.nth (acList, id), ())
  in
    v
  end
end
