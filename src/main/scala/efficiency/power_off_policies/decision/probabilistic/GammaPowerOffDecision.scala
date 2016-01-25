package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{Job, CellState}
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{GammaDistributionImpl, ExponentialDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class GammaPowerOffDecision(threshold : Double, windowSize: Int) extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    //TODO: Calculate Ts
    val ts = 130.0
    var should = false
    var interArrivalAvg = 0.0
    var memAvg = 0.0
    var cpuAvg = 0.0
    var allPastTuples = Seq[Tuple2[Double, Job]]()
    val interArrival = Seq[Double]()
    val memConsumed = Seq[Double]()
    val cpuConsumed = Seq[Double]()
    cellState.simulator.schedulers.map(_._2).foreach(_.cleanPastJobs(windowSize+1))
    val multipleListTuples = cellState.simulator.schedulers.map(_._2).map(_.pastJobs).map(_.values).map(_.toSeq)
    /*    for (tuples <- multipleListTuples){
          for (tuple <- tuples){
            var time = tuple._1
            var job = tuple._2
          }
        }*/
    for(tuples <- multipleListTuples){
      allPastTuples :+ tuples
    }
    allPastTuples.sortBy(_._1)
    if(allPastTuples.length >= windowSize+1){
      allPastTuples = allPastTuples.slice(allPastTuples.length-(windowSize+2), allPastTuples.length-1)
    }
    for(i <- 1 to allPastTuples.length-1){
      interArrival :+ (allPastTuples(i)._1 - allPastTuples(i-1)._1)
      memConsumed :+ allPastTuples(i)._2.numTasks*allPastTuples(i)._2.memPerTask
      cpuConsumed :+ allPastTuples(i)._2.numTasks*allPastTuples(i)._2.cpusPerTask
    }
    interArrivalAvg = interArrival.sum / interArrival.length
    memAvg = memConsumed.sum / memConsumed.length
    cpuAvg = cpuConsumed.sum / cpuConsumed.length
    if(interArrivalAvg > 0.0 && memAvg > 0.0 && cpuAvg > 0.0){
      var alphaCpu = cellState.availableCpus / cpuAvg
      var alphaMem = cellState.availableMem / memAvg
      val dist = new GammaDistributionImpl((alphaCpu+alphaMem/2), interArrivalAvg)
      val prob = dist.cumulativeProbability(ts)
      should = prob <= threshold
    }
    should
  }

  override val name: String = "exponential-power-off-decision"
}
