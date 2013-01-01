/* Copyright (C) 2009 Matthew Fluet.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 */

#if (defined (MLTON_GC_INTERNAL_TYPES))

/* must agree w/ cardSizeLog2 in ssa-to-rssa.fun */
#define CARD_SIZE_LOG2 8
#define CARD_SIZE TWOPOWER(CARD_SIZE_LOG2)

typedef uint8_t GC_cardMapElem;
typedef uint8_t GC_crossMapElem;

typedef GC_cardMapElem *GC_cardMap;
typedef GC_crossMapElem *GC_crossMap;

typedef size_t GC_cardMapIndex;
typedef size_t GC_crossMapIndex;
#define CARD_MAP_ELEM_SIZE sizeof(GC_cardMapElem)
#define CROSS_MAP_ELEM_SIZE sizeof(GC_crossMapElem)
#define CROSS_MAP_EMPTY ((GC_crossMapElem)255)
#define CROSS_MAP_OFFSET_SCALE 4
#define FMTCME "%"PRIu8

struct GC_generationalMaps {
  /* cardMap is an array with cardinality equal to the size of the
   * heap divided by card size.  Each element in the array is
   * interpreted as a boolean; true indicates that some mutable field
   * of some object in the corresponding card in the heap has been
   * written since the last minor GC; hence, the corresponding card
   * must be traced at the next minor GC.
   */
  GC_cardMap cardMap;
  GC_cardMap cardMapAbsolute;
  GC_cardMapIndex cardMapLength;
  /* crossMap is an array with cardinality equal to the size of the
   * heap divided by card size.  Each element in the array is
   * interpreted as a byte offset (scaled by CARD_MAP_OFFSET_SCALE);
   * the offset indicates the start of the last object in the
   * corresponding card from the start of the card.
   */
  GC_crossMap crossMap;
  GC_crossMapIndex crossMapLength;
  /* crossMapValidSize the size of the prefix of the old generation
   * for which the crossMap is valid.
   */
  size_t crossMapValidSize;
};

#endif /* (defined (MLTON_GC_INTERNAL_TYPES)) */

#if (defined (MLTON_GC_INTERNAL_FUNCS))

static void displayGenerationalMaps (GC_state s,
                                     struct GC_generationalMaps *generational,
                                     FILE *stream);

static inline GC_cardMapIndex sizeToCardMapIndex (size_t z);
static inline size_t cardMapIndexToSize (GC_cardMapIndex i);
static inline GC_cardMapIndex pointerToCardMapIndexAbsolute (pointer p);
static inline GC_cardMapElem *pointerToCardMapAddr (GC_state s, pointer p);

static inline GC_crossMapIndex sizeToCrossMapIndex (size_t z);

static inline bool isCardMarked (GC_state s, pointer p);
static inline void markCard (GC_state s, pointer p);
static inline void markIntergenerationalPointer (GC_state s, pointer *pp);
static inline void markIntergenerationalObjptr (GC_state s, objptr *opp);

static inline void setCardMapAbsolute (GC_state s);
static inline pointer getCrossMapCardStart (GC_state s, pointer p);

static inline size_t sizeofCardMap (GC_state s, size_t heapSize);
static inline GC_cardMapIndex lenofCardMap (GC_state s, size_t cardMapSize);
static inline size_t sizeofCrossMap (GC_state s, size_t heapSize);
static inline GC_crossMapIndex lenofCrossMap (GC_state s, size_t crossMapSize);
static size_t sizeofCardMapAndCrossMap (GC_state s, size_t heapSize);
static size_t invertSizeofCardMapAndCrossMap (GC_state s, size_t heapWithMapsSize);

static inline void clearCardMap (GC_state s);
static inline void clearCrossMap (GC_state s);
static inline void clearCardMapAndCrossMap (GC_state s);
static void setCardMapAndCrossMap (GC_state s);

#if ASSERT
static bool isCrossMapOk (GC_state s);
#endif
static void updateCrossMap (GC_state s);

#endif /* (defined (MLTON_GC_INTERNAL_FUNCS)) */
