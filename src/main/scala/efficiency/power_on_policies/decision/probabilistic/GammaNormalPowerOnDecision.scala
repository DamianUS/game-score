package efficiency.power_on_policies.decision.probabilistic

import ClusterSchedulingSimulation.{ClaimDelta, CellState, Job}
import efficiency.power_off_policies.decision.PowerOffDecision
import efficiency.power_on_policies.decision.PowerOnDecision
import org.apache.commons.math.distribution.{GammaDistributionImpl, NormalDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class GammaNormalPowerOnDecision(normalThreshold: Double, threshold : Double, windowSize: Int) extends PowerOnDecision{

  override def shouldPowerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Boolean = {
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    var should = false
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
      //FIXME: No deberíamos usar allPastTuples(i)._2.taskDuration porque en la realidad no funciona así. Si esto funciona bien, modelar una normal
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
    if(interArrivalAvg > 0.0 && memAvg > 0.0 && cpuAvg > 0.0 && memDeviation > 0.0 && cpuDeviation > 0.0 && interArrivalDeviation > 0.0){
      val alphaCpu = (cellState.availableCpus + cpuFree + cellState.numberOfMachinesTurningOn*cellState.cpusPerMachine) / new NormalDistributionImpl(cpuAvg, cpuDeviation).inverseCumulativeProbability(normalThreshold)
      val alphaMem = (cellState.availableMem + cpuFree + cellState.numberOfMachinesTurningOn*cellState.memPerMachine) / new NormalDistributionImpl(memAvg, memDeviation).inverseCumulativeProbability(normalThreshold)
      //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
      if(alphaCpu > 0.0 || alphaMem > 0.0) {
        val dist = new GammaDistributionImpl(Math.min(alphaCpu, alphaMem), new NormalDistributionImpl(interArrivalAvg, interArrivalDeviation).inverseCumulativeProbability(normalThreshold))
        val prob = dist.cumulativeProbability(cellState.powerOnTime)
        should = prob > threshold
        /*if(should)
          println(("La política : %s decide encender con una probabilidad de %f frente al threshold %f con una disponibilidad de cpu de %f quedando %d máquinas encendidas").format(name, prob, threshold, cellState.availableCpus/cellState.onCpus, cellState.numberOfMachinesOn))
        else
          println(("La política : %s decide no encender con una probabilidad de %f frente al threshold %f con una disponibilidad de cpu de %f quedando %d máquinas encendidas").format(name, prob, threshold, cellState.availableCpus/cellState.onCpus, cellState.numberOfMachinesOn))*/
      }
    }
    should
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

  override val name: String = "gamma-normal-power-on-decision"
}
