package efficiency.power_off_policies.decision.deterministic.load

import ClusterSchedulingSimulation.CellState
import efficiency.power_off_policies.decision.PowerOffDecision

/**
   * Created by dfernandez on 15/1/16.
   */
class LoadMaxPowerOffDecision(threshold : Double) extends PowerOffDecision{
  //Intución: Cuando el centro de datos esté menos cargado que el threshold, apaga
    override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
        threshold > Math.max(cellState.totalOccupiedCpus/cellState.totalCpus, cellState.totalOccupiedMem/cellState.totalMem)
    }

    override val name: String = ("load-max-power-off-decision-with-threshold:%f").format(threshold)
  }
