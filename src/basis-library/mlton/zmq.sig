signature MLTON_ZMQ =
sig

  (* Context Management *)

  type context

  val init : unit -> context
  val term : context -> unit

end
