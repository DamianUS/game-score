package efficiency.power_on_policies.action.unsatisfied

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}
import efficiency.power_on_policies.action.PowerOnAction

import scala.util.control.Breaks

/**
 * Created by dfernandez on 15/1/16.
 */
object DefaultPowerOnAction extends PowerOnAction{
  //FIXME: No tenemos en cuenta ni los conflicted delta ni el modo all or nothing, mejoras más adelante
  override def powerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Unit = {
    if(job!=null)
      job.turnOnRequests = job.turnOnRequests :+ cellState.simulator.currentTime
    var machinesToPowerOn = 0
    val machinesNeeded = Math.max((job.cpusStillNeeded / cellState.cpusPerMachine).ceil.toInt, (job.memStillNeeded / cellState.memPerMachine).ceil.toInt)
    if (cellState.numberOfMachinesOff >= machinesNeeded) {
      cellState.simulator.log(("There are enough machines turned off, turning on %d machines on %s policy").format(machinesNeeded, name))
      machinesToPowerOn = machinesNeeded
    }
    else if (cellState.numberOfMachinesOff > 0) {
      cellState.simulator.log(("There are not enough machines turned off, turning on %d machines on %s policy").format(cellState.numberOfMachinesOff, name))
      machinesToPowerOn = cellState.numberOfMachinesOff
    }
    else {
      cellState.simulator.log(("All machines are on, cant turn on any machines on %s policy").format(name))
    }
    //println(("Encendiendo %d maquinas por petición del job %d con %d tareas restantes del total de %d").format(machinesToPowerOn, job.id, job.unscheduledTasks, job.numTasks))
    powerOnMachines(cellState, machinesToPowerOn, schedType)
  }

  override val name: String = "default-power-on-action"
}
