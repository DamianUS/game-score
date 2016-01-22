package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.CellState
import efficiency.power_off_policies.decision.PowerOffDecision

import scala.util.Random

/**
  * Created by dfernandez on 15/1/16.
  */
object RandomPowerOffDecision extends PowerOffDecision{
   override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {Random.nextFloat() > 0.5}

   override val name: String = "random-power-off-decision"
 }
