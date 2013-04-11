(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure TransactionId : TRANSACTION_ID =
struct

  datatype trans_id = datatype RepTypes.trans_id

  fun newTxid () = TXID (ref false)

  fun isDone (TXID txst) = !txst

  fun force (TXID txst) =
    (Assert.assertAtomic' ("TransactionId.force", NONE);
     if !txst then raise Fail "TransactionId.force: already forced!"
     else txst := true)

end
