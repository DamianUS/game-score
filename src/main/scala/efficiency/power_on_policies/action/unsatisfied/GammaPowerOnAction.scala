package efficiency.power_on_policies.action.unsatisfied

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}
import efficiency.DistributionUtils
import efficiency.power_off_policies.decision.probabilistic.GammaNormalPowerOffDecision
import efficiency.power_on_policies.action.PowerOnAction
import org.apache.commons.math.distribution.{GammaDistributionImpl, NormalDistributionImpl}

import scala.util.control.Breaks

/**
 * Created by dfernandez on 15/1/16.
 */
class GammaPowerOnAction(normalThreshold: Double, threshold : Double, windowSize: Int) extends PowerOnAction with DistributionUtils{
  //FIXME: No tenemos en cuenta ni los conflicted delta ni el modo all or nothing, mejoras más adelante
  override def powerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Unit = {
    var numMachinesGamma = 0
    var gammaProbability = 10000.0;
    //println(("On : %f y ocupadas: %f").format(cellState.numberOfMachinesOn.toDouble/cellState.numMachines, cellState.numMachinesOccupied.toDouble/cellState.numMachines))
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    val allPastTuples = getPastTuples(cellState, windowSize)
    val jobAttributes = getJobAttributes(allPastTuples)
    var memFree = 0.0
    var cpuFree = 0.0
    if(job!=null)
      job.turnOnRequests = job.turnOnRequests :+ cellState.simulator.currentTime
    var machinesToPowerOn = 0
    var machinesNeeded = 0
    //FIXME: Necesitamos un método mejor que iterar de manera naive, algo como pivotar
    if (job!=null && job.unscheduledTasks > 0){
      machinesNeeded = Math.max((job.cpusStillNeeded / cellState.cpusPerMachine).ceil.toInt, (job.memStillNeeded / cellState.memPerMachine).ceil.toInt)
    }
    do{
      if(jobAttributes._1 > 0.0 && jobAttributes._2 > 0.0 && jobAttributes._3 > 0.0 && jobAttributes._4 > 0.0 && jobAttributes._5 > 0.0 && jobAttributes._6 > 0.0){
        numMachinesGamma += 1
        val alphaCpu = (cellState.availableCpus + cellState.numberOfMachinesTurningOn*cellState.cpusPerMachine + cpuFree + cellState.cpusPerMachine*numMachinesGamma) / Math.max(0.01, getNormalDistributionInverseCummulativeProbability(jobAttributes._5, jobAttributes._6, normalThreshold))
        val alphaMem = (cellState.availableMem + cellState.numberOfMachinesTurningOn*cellState.cpusPerMachine + memFree + cellState.memPerMachine*numMachinesGamma) / Math.max(0.01, getNormalDistributionInverseCummulativeProbability(jobAttributes._3, jobAttributes._4, normalThreshold))
        var beta = getNormalDistributionInverseCummulativeProbability(jobAttributes._1, jobAttributes._2, 1-normalThreshold)
        if (true)
          beta = jobAttributes._1
        //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
        if(alphaCpu > 0.0 || alphaMem > 0.0) {
          val prob = getGammaDistributionCummulativeProbability( Math.min(alphaCpu,alphaMem), beta, cellState.powerOnTime)
          gammaProbability = prob
        }
      }
    }while(jobAttributes._1 > 0.0 && jobAttributes._2 > 0.0 && jobAttributes._3 > 0.0 && jobAttributes._4 > 0.0 && jobAttributes._5 > 0.0 && jobAttributes._6 > 0.0 && gammaProbability > threshold && (numMachinesGamma + machinesNeeded) < cellState.numMachines)
    machinesNeeded = machinesNeeded + numMachinesGamma
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
    //println(("Encendiendo %d máquinas por petición del job %d con %d tareas restantes del total de %d quedando %d máquinas apagadas").format(machinesToPowerOn, job.id, job.unscheduledTasks, job.numTasks, cellState.numberOfMachinesOff))
    powerOnMachines(cellState, machinesToPowerOn, schedType)
  }

  override val name: String = ("gamma-normal-power-on-action-with-normal-threshold:%f-and-gamma-threshold:%f-and-window-size:%d").format(normalThreshold,threshold,windowSize)
}
