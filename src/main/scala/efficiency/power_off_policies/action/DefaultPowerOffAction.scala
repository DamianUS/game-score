package efficiency.power_off_policies.action

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}

import scala.util.control.Breaks

/**
 * Created by dfernandez on 15/1/16.
 */
object DefaultPowerOffAction extends PowerOffAction{
  override def powerOff(cellState: CellState, machineId: Int): Unit = {
    assert(cellState.machinePowerState(machineId) == 1, ("The machine with ID %i is not powered on when arriving to power off policy: %s").format(machineId, name))
    if(cellState.allocatedCpusPerMachine(machineId) == 0.0 && cellState.allocatedMemPerMachine(machineId) == 0.0) {
      cellState.powerOffMachine(machineId)
      cellState.simulator.log(("Shutting down the machine with machine ID : %i in the power off policy : %s").format(machineId, name))
    }
    else{
      cellState.simulator.log(("Can not shut down the machine with machine ID : %i in the power off policy : %s because it has allocated %f cpus and %f mem").format(machineId, name, cellState.allocatedCpusPerMachine(machineId), cellState.allocatedMemPerMachine(machineId)))
    }
  }

  override val name: String = "default-power-off-action"
}
