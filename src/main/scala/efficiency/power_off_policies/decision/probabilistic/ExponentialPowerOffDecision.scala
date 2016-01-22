package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.CellState
import efficiency.power_off_policies.decision.PowerOffDecision

import scala.util.Random

/**
 * Created by dfernandez on 22/1/16.
 */
class ExponentialPowerOffDecision extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    val multipleListTuples = cellState.simulator.schedulers.map(_._2).map(_.pastJobs).map(_.values).map(_.toSeq)
    for (tuples <- multipleListTuples){
      for (tuple <- tuples){
        var time = tuple._1
        var job = tuple._2
      }
    }
    for(tuples <- multipleListTuples){
      val times = tuples.map(_._1)
      val jobs = tuples.map(_._2)
    }
  }

  override val name: String = "exponential-power-off-decision"
}
