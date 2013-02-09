(* arbitrator.sig
 *
 * 2013 KC Sivaramakrishnan
 *
 * Stabilizer graph management.
 *
 *)

signature ARBITRATOR =
sig
  val processAdd    : {action: RepTypes.action, prevAction: RepTypes.action option} -> unit
  val processCommit : {action: RepTypes.action, pushResult: RepTypes.msg -> unit} -> unit
  val markCycleDepGraph : RepTypes.action -> unit
end
