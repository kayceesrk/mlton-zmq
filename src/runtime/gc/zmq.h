/* Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */

#if (defined (MLTON_GC_INTERNAL_BASIS))

PRIVATE C_Errno_t(C_Int_t) GC_zmqSend (GC_state s, pointer msg, pointer prefix,
                                       C_ZMQ_Socket_t sock, C_Int_t flags);

#endif /* (defined (MLTON_GC_INTERNAL_BASIS)) */
