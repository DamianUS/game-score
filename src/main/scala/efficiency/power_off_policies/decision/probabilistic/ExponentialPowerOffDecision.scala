package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{Job, CellState}
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{ExponentialDistributionImpl, ExponentialDistribution}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by dfernandez on 22/1/16.
 */
class ExponentialPowerOffDecision(threshold : Double, windowSize: Int) extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    //TODO: Calculate Ts
    val ts = 130.0
    var should = false
    var avg = 0.0
    var allPastTimes = ListBuffer[Double]()
    var interArrival = Seq[Double]()
    cellState.simulator.schedulers.map(_._2).foreach(_.cleanPastJobs(windowSize+1))
    var pastJobsMaps = Map[Long, Tuple2[Double, Job]]()

    for (mapElement <- cellState.simulator.schedulers.map(_._2).map(_.pastJobs)){
      pastJobsMaps = pastJobsMaps ++ mapElement
    }
    allPastTimes = allPastTimes ++ pastJobsMaps.map(_._2).map(_._1).toSeq
    allPastTimes.sorted
    if(allPastTimes.length >= windowSize+1){
      allPastTimes = allPastTimes.slice(allPastTimes.length-(windowSize+2), allPastTimes.length-1)
    }
    for( i <- 1 to allPastTimes.length-1){
      interArrival = interArrival :+ (allPastTimes(i) - allPastTimes(i-1))
    }
    avg = interArrival.sum / interArrival.length
    if(avg > 0.0){
      val dist = new ExponentialDistributionImpl(avg)
      val prob = dist.cumulativeProbability(ts)
      should = prob <= threshold
    }
    should
  }

  override val name: String = "exponential-power-off-decision"
}
