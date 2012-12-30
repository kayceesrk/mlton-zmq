/* Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */

#if (defined (MLTON_GC_INTERNAL_TYPES))

typedef C_Pointer_t ZMQ_Context;

#endif /* (defined (MLTON_GC_INTERNAL_TYPES)) */

#if (defined (MLTON_GC_INTERNAL_BASIS))

PRIVATE C_Errno_t(ZMQ_Context) ZMQ_init (void);
PRIVATE C_Errno_t(C_Int_t) ZMQ_term (ZMQ_Context context);

#endif /* (defined (MLTON_GC_INTERNAL_BASIS)) */
