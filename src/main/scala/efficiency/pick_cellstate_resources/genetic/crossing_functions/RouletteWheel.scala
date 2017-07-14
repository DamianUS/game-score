package efficiency.pick_cellstate_resources.genetic.crossing_functions

import ClusterSchedulingSimulation.{CellState, Job, Seed}
import efficiency.pick_cellstate_resources.genetic.fitness_functions.FitnessFunction

import scala.collection.mutable.{IndexedSeq, ListBuffer}
import scala.collection.Searching._

/**
  * Created by dfernandez on 11/1/16.
  */
object RouletteWheel extends CrossingFunctionSelector{
  val randomNumberGenerator = new util.Random(Seed())

  override def select(chromosomes: ListBuffer[ListBuffer[Int]], naturalFitnessScores: Boolean, selectionSize: Int, fitnessFunction: FitnessFunction, job: Job, cellState: CellState): ListBuffer[ListBuffer[Int]] = {
    assert(chromosomes.size > 1, "Needed at least two chromosomes")
    assert(chromosomes.size >= selectionSize, "The chromosomes size must be higher than the selection")
    val cumulativeFitnesses = new Array[Double](chromosomes.size)
    cumulativeFitnesses(0) = getAdjustedFitness(fitnessFunction.evaluate(chromosomes(0),job,cellState), naturalFitnessScores)
    for (i <- 1 until chromosomes.size){
      val fitness = getAdjustedFitness(fitnessFunction.evaluate(chromosomes(i),job,cellState), naturalFitnessScores)
      cumulativeFitnesses(i) = cumulativeFitnesses(i-1) + fitness
    }

    val selection = new scala.collection.mutable.ListBuffer[ListBuffer[Int]]
    for (i <- 0 until selectionSize){
      //This was added to disallow repeated chromosomes
      var added = false
      while(!added){
        val randomFitness = randomNumberGenerator.nextDouble() * cumulativeFitnesses(cumulativeFitnesses.size - 1)
        import java.util
        var index = util.Arrays.binarySearch(cumulativeFitnesses, randomFitness)
        //Check if those two are working in the same manner in order to remove java code
        var index2 = cumulativeFitnesses.search(randomFitness)
        if(index < 0){
          index = Math.abs(index + 1)
        }
        if(selection.isEmpty || !selection.contains(chromosomes(index))){
          selection += chromosomes(index)
          added = true
        }
      }
    }
    selection
  }




  def getAdjustedFitness(rawFitness: Double, naturalFitness: Boolean) = {
    if (naturalFitness) rawFitness
    else { // If standardised fitness is zero we have found the best possible
      // solution.  The evolutionary algorithm should not be continuing
      // after finding it.
      if (rawFitness == 0) Double.MaxValue
      else 1 / rawFitness
    }
  }

  override val name: String = "roulette-wheel"
}
