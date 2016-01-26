package efficiency.power_on_policies.action.margin

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}
import efficiency.power_on_policies.action.PowerOnAction

import scala.util.control.Breaks

/**
 * Created by dfernandez on 15/1/16.
 */
class PowerOnMarginPercAvailableAction(resourcesPercentageMargin : Double) extends PowerOnAction{
  //On this policy, we will turn on the machines needed to maintain a percentage of data center resources on once satisfied unscheduled tasks
  //FIXME: No tenemos en cuenta ni los conflicted delta ni el modo all or nothing, mejoras más adelante
  override def powerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Unit = {
    assert(resourcesPercentageMargin >= 0.0 && resourcesPercentageMargin <= 1.0, ("Security margin in %s policy must be between 0.0 and 1.0").format(name))
    var machinesToPowerOn = 0
    val machinesNeeded = numMachinesNeeded(cellState, job)
    assert(machinesNeeded >= 0, ("The number of machines that should be powered on is lesser than 0 in %s policy").format(name))
    if(cellState.numberOfMachinesOff >= machinesNeeded){
      cellState.simulator.log(("There are enough machines turned off, turning on %i machines on %s policy").format(machinesNeeded, name))
      machinesToPowerOn = machinesNeeded
    }
    else if(cellState.numberOfMachinesOff > 0){
      cellState.simulator.log(("There are not enough machines turned off, turning on %i machines on %s policy").format(cellState.numberOfMachinesOff, name))
      machinesToPowerOn = cellState.numberOfMachinesOff
    }
    else{
      cellState.simulator.log(("All machines are on, cant turn on any machines on %s policy").format(name))
    }
    val loop = new Breaks;
    loop.breakable {
      for (i <- cellState.machinesLoad.length - 1 to 0 by -1) {
        if (machinesToPowerOn == 0){
          loop.break
        }
        if (cellState.isMachineOff(cellState.machinesLoad(i))) {
          cellState.powerOnMachine(cellState.machinesLoad(i))
          machinesToPowerOn -= 1
        }
      }
    }
    assert(machinesToPowerOn == 0, ("Something went wrong on %s policy, there are still %i machines to turn on after powering on machines").format(name, machinesToPowerOn))
  }

  override val name: String = "power-on-percentage-resources-free-margin-action"

  def numMachinesNeeded(cellState: CellState, job: Job): Int = {
    val machinesCpuNeeded = Math.max(0, ((resourcesPercentageMargin * cellState.numberOfMachinesOn * cellState.cpusPerMachine - (cellState.availableCpus - job.cpusStillNeeded))/cellState.cpusPerMachine).ceil.toInt)
    val machinesMemNeeded = Math.max(0, ((resourcesPercentageMargin * cellState.numberOfMachinesOn * cellState.memPerMachine - (cellState.availableMem - job.memStillNeeded))/cellState.memPerMachine).ceil.toInt)
    Math.max(machinesCpuNeeded, machinesMemNeeded)
  }
}


