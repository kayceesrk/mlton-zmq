(* This test --fails-- *)

val f = print
val ser_f = MLton.serialize f
val f' : string -> unit = MLton.deserialize ser_f
val _ = f' (Int.toString 0)
