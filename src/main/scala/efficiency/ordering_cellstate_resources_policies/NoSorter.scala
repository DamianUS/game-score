package efficiency.ordering_cellstate_resources_policies

import ClusterSchedulingSimulation.CellState

/**
 * Created by dfernandez on 8/1/16.
 */
// Initially defined as an object for simplicity of usage
// TODO: Become a class if necessary
object NoSorter extends CellStateResourcesSorter{
  override def orderResources(cellstate: CellState): Unit = {
    val load = new Array[Int](cellstate.allocatedCpusPerMachine.length)
    for ( i <-0 to load.length-1){
      load(i)=i
    }
    // Now keys should be ordered by load
    cellstate.machinesLoad = load
  }

  override def getName(): String = {
    "no-sorter"
  }
}
