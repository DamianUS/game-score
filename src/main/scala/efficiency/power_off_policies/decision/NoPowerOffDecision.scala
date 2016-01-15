package efficiency.power_off_policies.decision

import ClusterSchedulingSimulation.CellState

/**
 * Created by dfernandez on 15/1/16.
 */
object NoPowerOffDecision extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = false

  override val name: String = "no-power-off-decision"
}
