package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{Job, CellState}
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{GammaDistributionImpl, ExponentialDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class GammaPowerOffDecision(threshold : Double, windowSize: Int) extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    println(("On : %f y ocupadas: %f").format(cellState.numberOfMachinesOn.toDouble/cellState.numMachines, cellState.numMachinesOccupied.toDouble/cellState.numMachines))
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    val ts = 130.0
    var should = false
    var interArrivalAvg = 0.0
    var memAvg = 0.0
    var cpuAvg = 0.0
    var allPastTuples = Seq[Tuple2[Double, Job]]()
    var interArrival = Seq[Double]()
    var memConsumed = Seq[Double]()
    var cpuConsumed = Seq[Double]()
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
    }
    interArrivalAvg = interArrival.sum / interArrival.length
    memAvg = memConsumed.sum / memConsumed.length
    cpuAvg = cpuConsumed.sum / cpuConsumed.length
    if(interArrivalAvg > 0.0 && memAvg > 0.0 && cpuAvg > 0.0){
      val alphaCpu = cellState.availableCpus / cpuAvg
      val alphaMem = cellState.availableMem / memAvg
      //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
      val dist = new GammaDistributionImpl( (alphaCpu+alphaMem)/2, interArrivalAvg)
      val prob = dist.cumulativeProbability(ts)
      should = prob <= threshold
    }
    should
  }

  override val name: String = "exponential-power-off-decision"
}
