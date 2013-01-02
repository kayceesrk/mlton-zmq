/* Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */

pointer GC_serialize (GC_state s, pointer p, GC_header header) {
  size_t objectClosureSize;
  pointer buffer;
  CopyObjectMap *e, *tmp;

  assert (isPointer (p) && isPointerInHeap (s, p));

  objectClosureSize = GC_size (s, p);
  buffer = GC_arrayAllocate (s, 0, objectClosureSize, header);

  s->forwardState.toStart = s->forwardState.back = buffer;
  s->forwardState.toLimit = (pointer)((char*)buffer + objectClosureSize);

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
  translateRange (s, buffer, s->heap.start, BASE_ADDR, objectClosureSize);

  return buffer;
}

pointer GC_deserialize (GC_state s, pointer p) {
  pointer frontier, newFrontier, result;
  size_t bytesRequested;

  bytesRequested = GC_getArrayLength (p);
  if (not hasHeapBytesFree (s, 0, bytesRequested)) {
    enter (s);
    performGC (s, 0, bytesRequested, FALSE, TRUE);
    leave (s);
  }
  frontier = s->frontier;
  newFrontier = frontier + bytesRequested;
  assert (isFrontierAligned (s, newFrontier));
  s->frontier = newFrontier;

  //Copy data and translate
  GC_memcpy (p, frontier, bytesRequested);
  translateRange (s, frontier, BASE_ADDR, s->heap.start, bytesRequested);
  result = advanceToObjectData (s, frontier);
  return result;
}
