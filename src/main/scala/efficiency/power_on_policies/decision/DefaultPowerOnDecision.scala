package efficiency.power_on_policies.decision

import ClusterSchedulingSimulation.{ClaimDelta, Job, CellState}

/**
 * Created by dfernandez on 15/1/16.
 */
object DefaultPowerOnDecision extends PowerOnDecision{
  override def shouldPowerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Boolean = {
    job!=null && job.unscheduledTasks > 0 && (job.turnOnRequests.length <=1 || (cellState.simulator.currentTime - job.turnOnRequests(job.turnOnRequests.length-1)) > cellState.powerOnTime*1.05)
  }

  override val name: String = "default-power-on-decision"
}
