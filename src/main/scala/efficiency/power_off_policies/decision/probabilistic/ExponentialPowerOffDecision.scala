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
    val dangerousPastTuples = allPastTuples.filter(pastTuple => (pastTuple._2.numTasks * pastTuple._2.cpusPerTask > cellState.availableCpus) || (pastTuple._2.numTasks * pastTuple._2.memPerTask > cellState.availableMem))
    //val jobAttributes = getJobAttributes(allPastTuples)
    if(dangerousPastTuples.size > 0){
      var interArrival= 0.0
      if(dangerousPastTuples.size > 1){
        interArrival = dangerousPastTuples(0)._1
      }
      else{
        interArrival = generateJobAtributes(dangerousPastTuples)._1
      }
      if(interArrival > 0.0){
        val lastDangerousTuple = dangerousPastTuples.maxBy(tuple => tuple._1)
        val prob = getExponentialDistributionCummulativeProbability(interArrival, Math.max(ts, cellState.simulator.currentTime - lastDangerousTuple._1))
        should = prob >= threshold
      }
    }

    should
  }

  override val name: String = ("exponential-power-off-decision-with-ts-:%f-threshold:%f-and-window-size:%d").format(ts,threshold,windowSize)
}
