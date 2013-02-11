(* Copyright (C) 2013 KC Sivaramakrishnan.
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

open Dml

signature ATOMIC_COUNTER =
sig
  type 'a t
  val new       : {increment: 'a * 'a -> 'a, init: 'a} -> 'a t
  val increment : {counter: 'a t, delta: 'a} -> {touchForResult: unit -> 'a}
end

structure AtomicCounter :> ATOMIC_COUNTER =
struct
  datatype 'a t = PORTS of {reqChan: 'a chan, resChan: 'a chan}

  val id = ref 0

  fun new {increment, init} =
  let
    val _ = MLton.Thread.atomicBegin ()
    val newId = !id
    val _ = id := newId + 1
    val _ = MLton.Thread.atomicEnd ()

    val reqChan = channel ("AC_REQ_"^(Int.toString newId))
    val resChan = channel ("AC_RES_"^(Int.toString newId))
    val state = ref NONE

    fun core () =
    let
      val delta = recv reqChan
      val oldValue = case !state of
                   NONE => raise Fail "AtomicCounter: impossible"
                 | SOME oldValue =>
                     (state := SOME (increment (oldValue, delta));
                      oldValue)
      val _ = send (resChan, oldValue)
    in
      core ()
    end

    val _ = spawn (core)
  in
    PORTS {reqChan = reqChan, resChan = resChan}
  end

  fun increment {counter = PORTS {resChan, reqChan}, delta} =
    let
      val _ = send (reqChan, delta)
    in
      {touchForResult = fn () => recv resChan}
    end
end
