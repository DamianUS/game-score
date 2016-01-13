package efficiency.power_on_policies

import ClusterSchedulingSimulation.{Job, CellState}

/**
 * Created by dfernandez on 13/1/16.
 */
trait PowerOnPolicy {
  def powerOn(cellstate: CellState, job: Job)
  def getName() : String
}
