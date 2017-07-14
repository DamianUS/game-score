package efficiency.pick_cellstate_resources.genetic.fitness_functions

import ClusterSchedulingSimulation.{CellState, Job}

import scala.collection.mutable.ListBuffer


/**
 * Created by dfernandez on 11/1/16.
 */
object Makespan extends FitnessFunction{

  override def evaluate(chromosome: ListBuffer[Int], job: Job, cellState: CellState): Double = {
    var makespan = 0.0
    if (job.workloadName == "Batch"){
      for(mID <- 0 until cellState.numMachines){
        if(chromosome(mID) == 1){
          if (job.taskDuration * cellState.machinesPerformance(mID) > makespan){
            makespan = job.taskDuration * cellState.machinesPerformance(mID)
          }
        }
      }
      makespan
    }
    else{
      job.taskDuration
    }
  }

  override val name: String = "makespan"
}
