(* Functions can also be serialized. *)
val f = fn x => x+1
val ser_f = MLton.serialize f
val f' : (int -> int) = MLton.deserialize ser_f
val _ = print (Int.toString (f' 1))
