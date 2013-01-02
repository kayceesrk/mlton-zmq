val v = 0
val ser_v = MLton.serialize v
val v' = MLton.deserialize ser_v
val _ = print ((Int.toString v') ^ "\n")
