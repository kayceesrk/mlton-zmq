/* Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */


C_Errno_t(ZMQ_Context) ZMQ_init (void) {
	return (ZMQ_Context) (Pointer) zmq_ctx_new ();
}

C_Errno_t(C_Int_t) ZMQ_term (ZMQ_Context context) {
	return (C_Int_t) zmq_ctx_destroy ((void*)context);
}
