/* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */

/* dstBuffer must have space for objectClosureSize bytes, and size ==
 * GC_size(p) */
void serializeHelper (GC_state s, pointer p, pointer dstBuffer, size_t objectClosureSize) {
  CopyObjectMap *e, *tmp;

  s->forwardState.toStart = s->forwardState.back = dstBuffer;
  s->forwardState.toLimit = (pointer)((char*)dstBuffer + objectClosureSize);

  objptr op = pointerToObjptr (p, s->heap.start);
  enter (s);
  copyObjptr (s, &op);
  foreachObjptrInRange (s, s->forwardState.toStart, &s->forwardState.back, copyObjptr, TRUE);
  leave (s);

  HASH_ITER (hh, s->copyObjectMap, e, tmp) {
    HASH_DEL (s->copyObjectMap, e);
    free (e);
  }
  s->copyObjectMap = NULL;
  translateRange (s, dstBuffer, dstBuffer, BASE_ADDR, objectClosureSize);
}

pointer GC_serialize (GC_state s, pointer p, GC_header header) {
  size_t objectClosureSize;
  pointer buffer;

  assert (isPointer (p) && isPointerInHeap (s, p));

  objectClosureSize = GC_size (s, p);
  buffer = GC_arrayAllocate (s, 0, objectClosureSize, header);
  serializeHelper (s, p, buffer, objectClosureSize);
  return buffer;
}

pointer deserializeHelper (GC_state s, pointer bufferStart, size_t bufferSize) {
  pointer frontier, newFrontier, result;

  if (not hasHeapBytesFree (s, 0, bufferSize)) {
    enter (s);
    performGC (s, 0, bufferSize, FALSE, TRUE);
    leave (s);
  }
  frontier = s->frontier;
  newFrontier = frontier + bufferSize;
  assert (isFrontierAligned (s, newFrontier));
  s->frontier = newFrontier;

  //Copy data and translate
  GC_memcpy (bufferStart, frontier, bufferSize);
  translateRange (s, frontier, BASE_ADDR, frontier, bufferSize);
  result = advanceToObjectData (s, frontier);
  return result;
}

pointer GC_deserialize (GC_state s, pointer p) {
  return deserializeHelper (s, p, GC_getArrayLength (p));
}

pointer GC_deserializeZMQMsg (GC_state s, C_ZMQ_Message_t p) {
  zmq_msg_t* msg = (zmq_msg_t*)p;
  void* data = zmq_msg_data (msg);
  size_t size = zmq_msg_size (msg);

  /* Drop prefix - prefix is always a Word8.word vector. */
  pointer prefix = (pointer)data + GC_ARRAY_HEADER_SIZE;
  size_t arrayLength = getArrayLength (prefix);
  /* Empty arrays have space for forwarding pointer. See sizeofArrayNoHeader.
   * */
  if (arrayLength == 0)
    arrayLength = OBJPTR_SIZE;
  size_t prefixSize = GC_ARRAY_HEADER_SIZE + arrayLength;

  pointer retVal = deserializeHelper (s, (pointer)data + prefixSize , size - prefixSize);

  int rc = zmq_msg_close (msg);
  assert (rc == 0 && "EFAULT");
  rc = rc; //Suppress GCC warnings

  free (msg); //Allocation was performed in MLton_ZMQ_Recv
  return retVal;
}
