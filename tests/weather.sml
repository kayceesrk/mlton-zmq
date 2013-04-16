(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, Matthew Fluet, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

structure ZMQ = MLton.ZMQ

fun server context =
let
  val publisher = ZMQ.sockCreate (context, ZMQ.Pub)
  val _ = ZMQ.sockBind (publisher, "tcp://*:5556")
  fun randNat x = Word.toInt (Word.mod (MLton.Random.rand (), Word.fromInt x))
  fun loop () =
  let
    val zip = randNat 100000
    val temp = randNat 115
    val hum = randNat 60 + 10
    val prefix = MLton.serialize (Int.toString zip)
    val _ = ZMQ.sendWithPrefix (publisher, {zip = zip,temp=temp,hum=hum}, prefix)
  in
    loop ()
  end
in
  loop ()
end

fun client context zip =
let
  val subscriber = ZMQ.sockCreate (context, ZMQ.Sub)
  val _ = ZMQ.sockConnect (subscriber, "tcp://localhost:5556")
  val filter = MLton.serialize zip
  val _ = ZMQ.sockSetSubscribe (subscriber, filter)
  fun loop n =
    if n = 0 then ()
    else
      let
        val {temp, zip, hum} = ZMQ.recv (subscriber)
        val _ = print (concat [Int.toString zip, " ",
                              Int.toString temp, " ",
                              Int.toString hum, "\n"])
      in
        loop (n-1)
      end
in
  loop 100
end


val args::tl = CommandLine.arguments ()
val context = ZMQ.ctxNew ()
val _ = if args = "server" then server context else client context (hd tl)
