package efficiency.ordering_cellstate_resources_policies

import ClusterSchedulingSimulation.CellState

/**
 * Created by dfernandez on 8/1/16.
 */
object OrderDebugging {
    def main(args: Array[String]): Unit = {
      var cellstate = new CellState(3, 2.0, 2.0, "resource-fit", "incremental");
      cellstate.allocatedCpusPerMachine(0)=0.0;
      cellstate.allocatedCpusPerMachine(1)=1.0;
      cellstate.allocatedCpusPerMachine(2)=1.9;
      BasicLoadSorter.orderResources(cellstate);
      println(cellstate.machinesLoad);
    }
}
