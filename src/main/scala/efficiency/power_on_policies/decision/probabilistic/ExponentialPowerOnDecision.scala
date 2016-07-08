package efficiency.power_on_policies.decision.probabilistic

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}
import efficiency.DistributionUtils
import efficiency.power_on_policies.decision.PowerOnDecision

/**
 * Created by dfernandez on 22/1/16.
 */
class ExponentialPowerOnDecision(threshold : Double, windowSize: Int, ts : Double = 130.0) extends PowerOnDecision with DistributionUtils{

  override def shouldPowerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Boolean = {
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

  override val name: String = ("exponential-power-on-decision-with-threshold:%f-and-window-size:%d").format(threshold,windowSize)
}
