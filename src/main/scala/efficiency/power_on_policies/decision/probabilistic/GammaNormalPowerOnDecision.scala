package efficiency.power_on_policies.decision.probabilistic

import ClusterSchedulingSimulation.{ClaimDelta, CellState, Job}
import efficiency.GammaUtils
import efficiency.power_off_policies.decision.PowerOffDecision
import efficiency.power_on_policies.decision.PowerOnDecision
import org.apache.commons.math.distribution.{GammaDistributionImpl, NormalDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class GammaNormalPowerOnDecision(normalThreshold: Double, threshold : Double, windowSize: Int) extends PowerOnDecision with GammaUtils{

  override def shouldPowerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Boolean = {
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    var should = false
    val allPastTuples = getPastTuples(cellState, windowSize)
    val jobAttributes = getJobAttributes(allPastTuples)
    var memFree = 0.0
    var cpuFree = 0.0
    if(jobAttributes._1 > 0.0 && jobAttributes._2 > 0.0 && jobAttributes._3 > 0.0 && jobAttributes._4 > 0.0 && jobAttributes._5 > 0.0 && jobAttributes._6 > 0.0){
      val alphaCpu = (cellState.availableCpus + cpuFree + cellState.numberOfMachinesTurningOn*cellState.cpusPerMachine) / getNormalDistributionInverseCummulativeProbability(jobAttributes._5, jobAttributes._6, normalThreshold)
      val alphaMem = (cellState.availableMem + cpuFree + cellState.numberOfMachinesTurningOn*cellState.memPerMachine) / getNormalDistributionInverseCummulativeProbability(jobAttributes._3, jobAttributes._4, normalThreshold)
      //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
      if(alphaCpu > 0.0 || alphaMem > 0.0) {
        var beta = getNormalDistributionInverseCummulativeProbability(jobAttributes._1, jobAttributes._2, 1-normalThreshold)
        if (true)
          beta = jobAttributes._1
        //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
        val prob = getGammaDistributionCummulativeProbability( Math.min(alphaCpu,alphaMem), beta , cellState.powerOnTime)
        should = prob > threshold
//        if(should)
//          println(("La política : %s decide encender con una probabilidad de %f frente al threshold %f con una disponibilidad de cpu de %f quedando %d máquinas encendidas").format(name, prob, threshold, cellState.availableCpus/cellState.onCpus, cellState.numberOfMachinesOn))
//        else
//          println(("La política : %s decide no encender con una probabilidad de %f frente al threshold %f con una disponibilidad de cpu de %f quedando %d máquinas encendidas").format(name, prob, threshold, cellState.availableCpus/cellState.onCpus, cellState.numberOfMachinesOn))
      }
    }
    should
  }

  override val name: String = "gamma-normal-power-on-decision"
}
