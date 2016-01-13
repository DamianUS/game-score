package efficiency.power_on_policies

import ClusterSchedulingSimulation.{Job, CellState}

/**
 * Created by dfernandez on 13/1/16.
 */
object NoPowerOnPolicy extends PowerOnPolicy{
  override def powerOn(cellstate: CellState, job: Job): Unit = {

  }

  override def getName(): String = {
    "no-power-on"
  }
}
