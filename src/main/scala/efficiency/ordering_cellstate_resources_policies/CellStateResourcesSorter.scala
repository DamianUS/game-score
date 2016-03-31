package efficiency.ordering_cellstate_resources_policies

import ClusterSchedulingSimulation.CellState

/**
 * Created by dfernandez on 8/1/16.
 */
trait CellStateResourcesSorter {
  def order(cellstate: CellState)
  def orderResources(cellState: CellState) : Unit ={
    if(!cellState.machinesLoadOrdered){
      order(cellState)
      cellState.machinesLoadOrdered = true
    }
  }
  val name: String
}
