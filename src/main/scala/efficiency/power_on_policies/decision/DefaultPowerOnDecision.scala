package efficiency.power_on_policies.decision

import ClusterSchedulingSimulation.{ClaimDelta, Job, CellState}

/**
 * Created by dfernandez on 15/1/16.
 */
object DefaultPowerOnDecision extends PowerOnDecision{
  override def shouldPowerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Boolean = job.unscheduledTasks > 0

  override val name: String = "default-power-on-decision"
}
