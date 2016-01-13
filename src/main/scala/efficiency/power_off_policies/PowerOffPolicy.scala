package efficiency.power_off_policies

import ClusterSchedulingSimulation.{CellState, Job}

/**
  * Created by dfernandez on 13/1/16.
  */
trait PowerOffPolicy {
   def powerOff(cellState: CellState, machineID: Int)
   def getName() : String
 }
