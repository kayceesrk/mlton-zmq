(* Copyright (C) 2013 KC Sivaramakrishnan.
 * Copyright (C) 1999-2008 Henry Cejtin, KC Sivaramakrishnan, Suresh
 *    Jagannathan, and Stephen Weeks.
 * Copyright (C) 1997-2000 NEC Research Institute.
 *
 * MLton is released under a BSD-style license.
 * See the file MLton-LICENSE for details.
 *)

signature DML =
sig
  type proxy
  type 'a chan

  (* never returns *)
  val startProxy : {frontend: string, backend: string} -> unit
  val connect : {sink: string, source: string} -> proxy
  val channel : proxy * string -> 'a chan
  val send : 'a chan * 'a -> unit
  val recv : 'a chan -> 'a
end
