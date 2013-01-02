val r = ref 1
val ser_r = MLton.serialize r
val _ = r := 2
val r' : int ref = MLton.deserialize ser_r

(* r' is still 1 since serialize copies mutable objects at the point of
 * serialization *)
val _ = print (Int.toString (!r'))
