(* thread.sml
 * 2004 Matthew Fluet (mfluet@acm.org)
 *  Ported to MLton threads.
 *)

(* thread.sml
 *
 * COPYRIGHT (c) 1995 AT&T Bell Laboratories.
 * COPYRIGHT (c) 1989-1991 John H. Reppy
 *)

structure ThreadID : THREAD_ID_EXTRA =
   struct
      structure Assert = LocalAssert(val assert = false)

      structure R = RepTypes


      datatype thread_id = datatype R.thread_id
      datatype thread_id' = datatype thread_id

      fun sameTid (TID {id = a,...}, TID {id = b, ...}) = a = b
      fun compareTid (TID {id = a, ...}, TID {id = b, ...}) = Int.compare (a, b)
      fun hashTid (TID {id, ...}) = Word.fromInt id

      fun tidToString (TID {id, ...}) =
         concat["[", StringCvt.padLeft #"0" 6 (Int.toString id), "]"]
      fun tidToInt (TID {id, ...}) = id
      fun tidToRev (TID {revisionId,...}) = !revisionId
      fun tidToNode (TID {currentNode, ...}) = currentNode
      fun tidNextActionNum (TID {actionNum,...}) =
      let
        val res = !actionNum + 1
        val _ = actionNum := res
      in
        res
      end

      fun tidSaveCont (TID {cont, ...}, doBeforeRestore) =
        MLton.Cont.callcc (fn k =>
          let
            val _ = cont := (fn () =>
              let
                val _ = doBeforeRestore ()
              in
                MLton.Cont.throw (k, ())
              end)
          in
            ()
          end)

      fun tidRestoreCont (TID {cont, revisionId, currentNode, ...}) =
      let
        val _ = revisionId := !revisionId + 1
        val _ = currentNode := NONE
      in
        (!cont) ()
      end

      fun exnHandler (_ : exn) = ()
      val defaultExnHandler = ref exnHandler

      fun new' n =
         TID {id = n,
              exnHandler = ref (!defaultExnHandler),
              props = ref [],
              currentNode = ref NONE,
              cont = ref (fn _ => ()),
              revisionId =  ref 0,
              actionNum = ref 0}
      local
         val tidCounter = ref 0
      in
         fun new () =
            let
               val _ = Assert.assertAtomic' ("ThreadID.newTid", NONE)
               val n = !tidCounter
               val _ = tidCounter := n + 1
            in
               new' n
            end

         fun reset () = tidCounter := 0
      end

      fun bogus s =
         let val n = CharVector.foldr (fn (c, n) => 2 * n - Char.ord c) 0 s
         in new' n
         end
   end
