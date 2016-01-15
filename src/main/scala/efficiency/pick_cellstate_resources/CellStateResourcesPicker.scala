package efficiency.pick_cellstate_resources

import ClusterSchedulingSimulation.{Scheduler, Job, CellState}

import scala.collection.mutable.IndexedSeq

/**
 * Created by dfernandez on 11/1/16.
 */
trait CellStateResourcesPicker {
  //Passing the Scheduler as a hook to update / accessing task-independent variables
  def pickResource(cellstate: CellState, job: Job, candidatePool: IndexedSeq[Int], remainingCandidates: Int) : Tuple4[Int, Int, Int, IndexedSeq[Int]]
  val name : String
}
