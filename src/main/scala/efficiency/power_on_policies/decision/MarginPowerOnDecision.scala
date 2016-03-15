package efficiency.power_on_policies.decision

import ClusterSchedulingSimulation.{CellState, ClaimDelta, Job}

/**
 * Created by dfernandez on 15/1/16.
 */
class MarginPowerOnDecision(securityMargin: Double) extends PowerOnDecision{
  override def shouldPowerOn(cellState: CellState, job: Job, schedType: String, commitedDelta: Seq[ClaimDelta], conflictedDelta: Seq[ClaimDelta]): Boolean = {
    assert(securityMargin >= 0.0 && securityMargin <= 1.0, ("Security margin in %s policy must be between 0.0 and 1.0").format(name))
    job.unscheduledTasks > 0 || mustPowerOn(cellState, job)
  }

  override val name: String = ("margin-power-on-decision-with-security-margin:%f").format(securityMargin)

  def mustPowerOn(cellState: CellState, job: Job): Boolean = {
    val machinesCpuNeeded = Math.max(0, ((securityMargin * cellState.numberOfMachinesOn * cellState.cpusPerMachine - (cellState.availableCpus - job.cpusStillNeeded))/cellState.cpusPerMachine).ceil.toInt)
    val machinesMemNeeded = Math.max(0, ((securityMargin * cellState.numberOfMachinesOn * cellState.memPerMachine - (cellState.availableMem - job.memStillNeeded))/cellState.memPerMachine).ceil.toInt)
    machinesCpuNeeded > 0 || machinesMemNeeded > 0
  }
}
