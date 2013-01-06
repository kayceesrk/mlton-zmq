/* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */

static void zmqFreeHelper (void* data, __attribute__((unused)) void* hint) {
  free (data);
}

C_Errno_t(C_Int_t) GC_zmqSend (GC_state s, pointer msg, pointer prefix,
                               C_ZMQ_Socket_t sock, C_Int_t flags) {
  size_t msgSize = GC_size (s, msg);
  size_t prefixSize = GC_size (s, prefix);
  pointer buffer = (pointer) malloc_safe (prefixSize + msgSize);

  //Prefix is always a Word8.word vector
  GC_memcpy (prefix - GC_ARRAY_HEADER_SIZE, buffer, prefixSize);
  serializeHelper (s, msg, buffer + prefixSize, msgSize);

  zmq_msg_t zmqMsg;
  int rc = zmq_msg_init_data (&zmqMsg, buffer, prefixSize + msgSize,
                              zmqFreeHelper, NULL);
  assert (rc == 0 && "ENOMEM");
  rc = rc; //Suppress GCC warning
  int retVal = zmq_msg_send (&zmqMsg, (void*)sock, flags);
  rc = zmq_msg_close (&zmqMsg);
  assert (rc == 0 && "EFAULT");
  return retVal;
}
