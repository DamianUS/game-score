package efficiency.power_on_policies

import ClusterSchedulingSimulation.{ClaimDelta, Job, CellState}
import efficiency.power_on_policies.action.PowerOnAction
import efficiency.power_on_policies.decision.PowerOnDecision

/**
 * Created by dfernandez on 13/1/16.
 */
trait PowerOnPolicy {
  var powerOnAction : PowerOnAction
  var powerOnDecisionPolicy : PowerOnDecision
  val name : String
  def powerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta] = Seq[ClaimDelta](), conflictedDelta: Seq[ClaimDelta] =Seq[ClaimDelta]()) = {
    assertSchedulerName(schedType)
    if(powerOnDecisionPolicy.shouldPowerOn(cellState, job, schedType, commitedDelta, conflictedDelta))
      powerOnAction.powerOn(cellState, job, schedType, commitedDelta, conflictedDelta)
  }
  def assertSchedulerName(schedType: String) = {
    assert(schedType == "mesos" || schedType == "omega" || schedType == "monolithic", ("Sched Type invalid in %s Power On Policy").format(name))
  }
}
