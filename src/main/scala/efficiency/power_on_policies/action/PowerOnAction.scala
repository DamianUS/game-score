package efficiency.power_on_policies.action

import ClusterSchedulingSimulation.{ClaimDelta, Job, CellState}

/**
 * Created by dfernandez on 15/1/16.
 */
trait PowerOnAction {
  def powerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta] = Seq[ClaimDelta](), conflictedDelta: Seq[ClaimDelta] =Seq[ClaimDelta]())
  def getName() : String
}
