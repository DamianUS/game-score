package efficiency.power_off_policies

import ClusterSchedulingSimulation.{CellState, ClusterSimulator}
import efficiency.power_off_policies.action.PowerOffAction
import efficiency.power_off_policies.decision.PowerOffDecision

/**
  * Created by dfernandez on 13/1/16.
  */
trait PowerOffPolicy {
  var powerOffAction : PowerOffAction
  var powerOffDecisionPolicy : PowerOffDecision
  val shouldPerformGlobalCheck : Boolean = false
  val globalCheckPeriod : Double = 300.0
  var initiated : Boolean = false
  def powerOff(cellState: CellState, machineID: Int)= {
    if(!initiated && shouldPerformGlobalCheck){
      initiated = true
      globalCheck(cellState)
    }
    if(powerOffDecisionPolicy.shouldPowerOff(cellState, machineID))
      powerOffAction.powerOff(cellState, machineID)
  }
  val name : String

  def globalCheck(cellState: CellState) :Unit ={
    var a = 0
    for (a <- 0 to cellState.numMachines-1){
      powerOff(cellState, a)
    }
    cellState.simulator.afterDelay(globalCheckPeriod){
        globalCheck(cellState)
    }
  }
}
