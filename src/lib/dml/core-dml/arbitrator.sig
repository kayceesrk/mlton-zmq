(* arbitrator.sig
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)

signature ARBITRATOR =
sig
  val startArbitrator : {sink: string, source: string, numPeers: int} -> unit
end
