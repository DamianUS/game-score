package efficiency.ordering_cellstate_resources_policies

import ClusterSchedulingSimulation.CellState

/**
 * Created by dfernandez on 8/1/16.
 */
// Initially defined as an object for simplicity of usage
// TODO: Become a class if necessary
object PowerStateLoadSorter extends CellStateResourcesSorter{
  override def orderResources(cellstate: CellState): Unit = {
    val load = collection.mutable.ListMap[Int, Double]();
    for ( i <-0 to cellstate.allocatedCpusPerMachine.length-1){
      if(cellstate.isMachineOn(i)){
        val cpuLoad = cellstate.allocatedCpusPerMachine(i)/(cellstate.totalCpus/cellstate.numMachines)
        val memLoad = cellstate.allocatedMemPerMachine(i)/(cellstate.totalMem/cellstate.numMachines)
        val calculatedLoad = Math.max(cpuLoad, memLoad)
        load += (i -> calculatedLoad)
      }
      else{
        // If machine is off, place it the last
        load += (i -> 2.0)
      }
    }
    val ordered = collection.mutable.LinkedHashMap(load.toList.sortWith( (x,y) => x._2 > y._2):_*)
    // Now keys should be ordered by load
    cellstate.machinesLoad = ordered.keys.toArray
  }
  override val name: String = "power-load-sorter"
}
