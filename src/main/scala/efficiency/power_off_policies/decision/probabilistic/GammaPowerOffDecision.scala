package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{Job, CellState}
import efficiency.{DistributionUtils, DistributionCache}
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{GammaDistributionImpl, ExponentialDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class GammaPowerOffDecision(threshold : Double, windowSize: Int, ts : Double = 130.0) extends PowerOffDecision with DistributionUtils{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    //println(("On : %f y ocupadas: %f").format(cellState.numberOfMachinesOn.toDouble/cellState.numMachines, cellState.numMachinesOccupied.toDouble/cellState.numMachines))
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    val allPastTuples = getPastTuples(cellState, windowSize)
    var should = false
    val jobAttributes = getJobAttributes(allPastTuples)

    if(jobAttributes._1 > 0.0 && jobAttributes._3 > 0.0 && jobAttributes._5 > 0.0){
      val alphaCpu = cellState.availableCpus / jobAttributes._5
      val alphaMem = cellState.availableMem / jobAttributes._3
      //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
      val prob = getGammaDistributionCummulativeProbability( (alphaCpu+alphaMem)/2, jobAttributes._1, ts)
      should = prob <= threshold
    }
    should
  }


  override val name: String = ("gamma-power-off-decision-with-gamma-threshold:%f-and-window-size:%d-and-ts:%f").format(threshold,windowSize,ts)
}
