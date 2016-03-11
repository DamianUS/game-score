package efficiency.power_off_policies

import ClusterSchedulingSimulation.CellState
import efficiency.power_off_policies.action.PowerOffAction
import efficiency.power_off_policies.decision.PowerOffDecision

/**
 * Created by dfernandez on 13/1/16.
 */
trait PowerOffPolicy {
  var powerOffAction : PowerOffAction
  var powerOffDecisionPolicy : PowerOffDecision
  def powerOff(cellState: CellState, machineID: Int)= {
    if(powerOffDecisionPolicy.shouldPowerOff(cellState, machineID))
      powerOffAction.powerOff(cellState, machineID)
  }
  val name : String
}
