package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{CellState, Job}
import efficiency.DistributionUtils
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{ExponentialDistribution, ExponentialDistributionImpl}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by dfernandez on 22/1/16.
 */
class ExponentialPowerOffDecision(threshold : Double, windowSize: Int, ts : Double = 130.0) extends PowerOffDecision with DistributionUtils{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    var should = false
    val allPastTuples = getPastTuples(cellState, windowSize)
    val jobAttributes = getJobAttributes(allPastTuples)
    if(jobAttributes._1 > 0.0){
      val prob = getExponentialDistributionCummulativeProbability( jobAttributes._1, ts)
      should = prob >= threshold
    }
    should
  }

  override val name: String = ("exponential-power-off-decision-with-ts-:%f-threshold:%f-and-window-size:%d").format(ts,threshold,windowSize)
}
