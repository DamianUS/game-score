package efficiency.power_on_policies.action.unsatisfied

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}
import efficiency.power_on_policies.action.PowerOnAction
import org.apache.commons.math.distribution.{GammaDistributionImpl, NormalDistributionImpl}

import scala.util.control.Breaks

/**
 * Created by dfernandez on 15/1/16.
 */
class GammaPowerOnAction(normalThreshold: Double, threshold : Double, windowSize: Int) extends PowerOnAction{
  //FIXME: No tenemos en cuenta ni los conflicted delta ni el modo all or nothing, mejoras más adelante
  override def powerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Unit = {
    var numMachinesGamma = 0
    var gammaProbability = 10000.0;
    var interArrivalAvg = 0.0
    var interArrivalDeviation = 0.0
    var memAvg = 0.0
    var cpuAvg = 0.0
    var memDeviation = 0.0
    var cpuDeviation = 0.0
    var allPastTuples = Seq[Tuple2[Double, Job]]()
    var interArrival = Seq[Double]()
    var memConsumed = Seq[Double]()
    var cpuConsumed = Seq[Double]()
    var memFree = 0.0
    var cpuFree = 0.0
    cellState.simulator.schedulers.map(_._2).foreach(_.cleanPastJobs(windowSize+1))
    var pastJobsMaps = Map[Long, Tuple2[Double, Job]]()
    for (mapElement <- cellState.simulator.schedulers.map(_._2).map(_.pastJobs)){
      pastJobsMaps = pastJobsMaps ++ mapElement
    }
    allPastTuples = allPastTuples ++ pastJobsMaps.map(_._2).toSeq
    allPastTuples = allPastTuples.sortBy(_._1)
    if(allPastTuples.length >= windowSize+1){
      allPastTuples = allPastTuples.slice(allPastTuples.length-(windowSize+2), allPastTuples.length-1)
    }
    for(i <- 1 to allPastTuples.length-1){
      interArrival = interArrival :+ (allPastTuples(i)._1 - allPastTuples(i-1)._1)
      memConsumed = memConsumed :+ allPastTuples(i)._2.numTasks*allPastTuples(i)._2.memPerTask
      cpuConsumed = cpuConsumed :+ allPastTuples(i)._2.numTasks*allPastTuples(i)._2.cpusPerTask
      //FIXME: No deberíamos usar allPastTuples(i)._2.taskDuration porque en la realidad no funciona así. Si esto funciona bien, modelar una normal. Meter un mapa para saber qué job está ocupando cuántos recursos en cada momento. Cálculo contrario
      /*if(allPastTuples(i)._2.unscheduledTasks > 0 && (allPastTuples(i)._2.lastSchedulingStartTime + allPastTuples(i)._2.taskDuration <= cellState.simulator.currentTime + cellState.powerOnTime)) {
        cpuFree += allPastTuples(i)._2.unscheduledTasks * allPastTuples(i)._2.cpusPerTask
        memFree += allPastTuples(i)._2.unscheduledTasks * allPastTuples(i)._2.memPerTask
      }*/
    }
    interArrivalAvg = interArrival.sum / interArrival.length
    interArrivalDeviation = stddev(interArrival)
    memAvg = memConsumed.sum / memConsumed.length
    memDeviation = stddev(memConsumed)
    cpuAvg = cpuConsumed.sum / cpuConsumed.length
    cpuDeviation = stddev(cpuConsumed)
    job.turnOnRequests = job.turnOnRequests :+ cellState.simulator.currentTime
    var machinesToPowerOn = 0
    var machinesNeeded = 0
    if (job.unscheduledTasks > 0){
      machinesNeeded = Math.max((job.cpusStillNeeded / cellState.cpusPerMachine).ceil.toInt, (job.memStillNeeded / cellState.memPerMachine).ceil.toInt)
    }
    do{
      if(interArrivalAvg > 0.0 && memAvg > 0.0 && cpuAvg > 0.0 && memDeviation > 0.0 && cpuDeviation > 0.0 && interArrivalDeviation > 0.0){
        numMachinesGamma += 1
        val alphaCpu = (cellState.availableCpus + cellState.numberOfMachinesTurningOn*cellState.cpusPerMachine + cpuFree + cellState.cpusPerMachine*numMachinesGamma) / new NormalDistributionImpl(cpuAvg, cpuDeviation).inverseCumulativeProbability(normalThreshold)
        val alphaMem = (cellState.availableMem + cellState.numberOfMachinesTurningOn*cellState.cpusPerMachine + memFree + cellState.memPerMachine*numMachinesGamma) / new NormalDistributionImpl(memAvg, memDeviation).inverseCumulativeProbability(normalThreshold)
        //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
        if(alphaCpu > 0.0 || alphaMem > 0.0) {
          val dist = new GammaDistributionImpl(Math.min(alphaCpu, alphaMem), new NormalDistributionImpl(interArrivalAvg, interArrivalDeviation).inverseCumulativeProbability(normalThreshold))
          val prob = dist.cumulativeProbability(cellState.powerOnTime)
          gammaProbability = prob
        }
      }
    }while(interArrivalAvg > 0.0 && memAvg > 0.0 && cpuAvg > 0.0 && memDeviation > 0.0 && cpuDeviation > 0.0 && interArrivalDeviation > 0.0 && gammaProbability > threshold)
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
    val loop = new Breaks;
    loop.breakable {
      for (i <- cellState.machinesLoad.length - 1 to 0 by -1) {
        if (machinesToPowerOn == 0) {
          loop.break
        }
        if (cellState.isMachineOff(cellState.machinesLoad(i))) {
          cellState.powerOnMachine(cellState.machinesLoad(i))
          machinesToPowerOn -= 1
        }
      }
    }
    //println(("Tras encender quedan %d máquinas apagadas").format(cellState.numberOfMachinesOff))
    assert(machinesToPowerOn == 0, ("Something went wrong on %s policy, there are still %d machines to turn on after powering on machines").format(name, machinesToPowerOn))
  }

  def mean[T](item:Traversable[T])(implicit n:Numeric[T]) = {
    n.toDouble(item.sum) / item.size.toDouble
  }

  def variance[T](items:Traversable[T])(implicit n:Numeric[T]) : Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total,item)=>{
      val itemDbl = n.toDouble(item)
      val square = math.pow(itemDbl - itemMean,2)
      total + square
    })
    sumOfSquares / count.toDouble
  }

  def stddev[T](items:Traversable[T])(implicit n:Numeric[T]) : Double = {
    math.sqrt(variance(items))
  }

  override val name: String = "gamma-power-on-action"
}
