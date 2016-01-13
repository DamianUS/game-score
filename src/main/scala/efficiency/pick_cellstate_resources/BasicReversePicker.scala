package efficiency.pick_cellstate_resources

import ClusterSchedulingSimulation.{Scheduler, CellState, Job}

import scala.collection.mutable.IndexedSeq
import scala.util.control.Breaks

/**
 * Created by dfernandez on 11/1/16.
 */
// This picker doesn't take into account yet shutted down machines nor capacity security margins nor performance
// TODO: Make a Quicksort-like strategy or pass a candidate index to iterate (e.g. the last successful candidate)
object BasicReversePicker extends CellStateResourcesPicker{
  override def pickResource(cellState: CellState, job: Job, candidatePool: IndexedSeq[Int], remainingCandidates: Int) = {
    var machineID = -1
    var numTries =0
    var remainingCandidatesVar = remainingCandidates
    val loop = new Breaks;
    loop.breakable {
      for( i <- cellState.machinesLoad.length-1 to 0 by -1){
        if (cellState.availableCpusPerMachine(cellState.machinesLoad(i)) >= job.cpusPerTask && cellState.availableMemPerMachine(cellState.machinesLoad(i)) >= job.memPerTask) {
          machineID=cellState.machinesLoad(i)
          loop.break;
        }
        else{
          numTries+=1
          remainingCandidatesVar -= 1 // This is irrelevant in this implementation, as derivable of numTries. I'll use it in quicksort-like implementations
        }

      }
    }
    new Tuple4(machineID, numTries, remainingCandidatesVar, candidatePool)
  }
  override def getName(): String = {
    "basic-reverse-picker"
  }
}
