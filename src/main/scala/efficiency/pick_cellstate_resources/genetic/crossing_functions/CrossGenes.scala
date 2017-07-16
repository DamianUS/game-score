package efficiency.pick_cellstate_resources.genetic.crossing_functions

import ClusterSchedulingSimulation.{CellState, Job, Seed}
import efficiency.pick_cellstate_resources.genetic.fitness_functions.FitnessFunction

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
  * Created by dfernandez on 11/1/16.
  */
object CrossGenes extends CrossingFunction{

  val randomNumberGenerator = new util.Random(Seed())

  override def cross(chromosome1: ListBuffer[Int], chromosome2: ListBuffer[Int]): (ListBuffer[Int], ListBuffer[Int]) = {
    val numTasksC1 = chromosome1.filter(_ == 1).size
    val numTasksC2 = chromosome2.filter(_ == 1).size
    assert(numTasksC1 == numTasksC2, "Number of tasks not valid")
    var genesToCross = 1 + randomNumberGenerator.nextInt(numTasksC1/2)
    var balance = 0
    var mIDProcessed = new collection.mutable.ListBuffer[Int]()
    //var log = new collection.mutable.ListBuffer[String]()
    val loop = new Breaks;
    loop.breakable {
      while (genesToCross > 0 || balance != 0) {
        val crossingPoint = randomNumberGenerator.nextInt(chromosome1.size - 1)
        for (mID <- crossingPoint until chromosome1.size) {
          if (!mIDProcessed.contains(mID)) {
            if (balance == 0 && chromosome1(mID) == 1 && chromosome2(mID) == 1) {
              //If the value of this gene is the same, it remains equal
              genesToCross -= 1
              //log += "balance 0 C1 1 los dos 1"
              //assert(chromosome1.filter(_ == 1).size == chromosome2.filter(_ == 1).size, "Mal en balance 0 C1 1 los dos 1")
            }
            else if (balance == 0 && chromosome1(mID) == 1 && chromosome2(mID) == 0) {
              //Case: Chromosome 1 loses one tasks, new balance is -1
              chromosome1(mID) = 0
              chromosome2(mID) = 1
              //log += "balance 0 C1 1 C2 0 con c1" + chromosome1.filter(_ == 1).size + " y c2 " + chromosome2.filter(_ == 1).size
              //assert(chromosome1.filter(_ == 1).size == (chromosome2.filter(_ == 1).size) - 2, "Mal en balance 0 C1 1 C2 0 con c1 " + chromosome1.filter(_ == 1).size + " y c2 " + chromosome2.filter(_ == 1).size)
              balance -= 1
              genesToCross -= 1
            }
            else if (balance == 0 && chromosome1(mID) == 0 && chromosome2(mID) == 1) {
              //Case: Chromosome 1 gains one tasks, new balance is 1
              chromosome1(mID) = 1
              chromosome2(mID) = 0
              //log += "balance 0 C2 1 C1 0 con c1 " + chromosome1.filter(_ == 1).size + " y c2 " + chromosome2.filter(_ == 1).size
              //assert((chromosome1.filter(_ == 1).size) - 2 == chromosome2.filter(_ == 1).size, "Mal en balance 0 C2 1 C1 0 con c1 " + chromosome1.filter(_ == 1).size + " y c2 " + chromosome2.filter(_ == 1).size)
              balance += 1
              genesToCross -= 1
            }
            else if (balance == 1 && chromosome1(mID) == 1 && chromosome2(mID) == 0) {
              //Chromosome 1 has gained 1 task. We must exchange one 1 from the C1 with one 0 from the C2
              //Case: Chromosome 1 loses one tasks, new balance is 0
              chromosome1(mID) = 0
              chromosome2(mID) = 1
              //log +=  "balance 1"
              //assert(chromosome1.filter(_ == 1).size == chromosome2.filter(_ == 1).size, "Mal en balance 1")
              balance -= 1
              genesToCross -= 1
            }
            else if (balance == -1 && chromosome1(mID) == 0 && chromosome2(mID) == 1) {
              //Chromosome 2 has gained 1 task. We must exchange one 1 from the C2 with one 0 from the C1
              //Case: Chromosome 1 gains one tasks, new balance is 0
              chromosome1(mID) = 1
              chromosome2(mID) = 0
              //log +=  "balance -1"
              //assert(chromosome1.filter(_ == 1).size == chromosome2.filter(_ == 1).size, "Mal en balance -1")
              balance += 1
              genesToCross -= 1
            }

            if (genesToCross <= 0 && balance == 0) {
              loop.break;
            }

          }
          mIDProcessed += mID
        }
      }
    }
    val validC1 = chromosome1.filter(_ == 1).size == numTasksC1
    val validC2 = chromosome2.filter(_ == 1).size == numTasksC1
    assert(validC1, "Chromosome 1 numTasks not valid in "+name)
    assert(validC2, "Chromosome 2 numTasks not valid in "+name)
    (chromosome1, chromosome2)
  }

  override val name: String = "cross-genes"
}
