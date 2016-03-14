package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{CellState, Job}
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{NormalDistributionImpl, ExponentialDistributionImpl, GammaDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class ExpNormPowerOffDecision(threshold : Double, windowSize: Int) extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    //println(("On : %f y ocupadas: %f").format(cellState.numberOfMachinesOn.toDouble/cellState.numMachines, cellState.numMachinesOccupied.toDouble/cellState.numMachines))
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    val ts = 130.0
    var should = false
    var interArrivalAvg = 0.0
    var memAvg = 0.0
    var cpuAvg = 0.0
    var memDeviation = 0.0
    var cpuDeviation = 0.0
    var allPastTuples = Seq[Tuple2[Double, Job]]()
    var interArrival = Seq[Double]()
    var memConsumed = Seq[Double]()
    var cpuConsumed = Seq[Double]()
    cellState.simulator.schedulers.map(_._2).foreach(_.cleanPastJobs(windowSize+1))
    var pastJobsMaps = Map[Long, Tuple2[Double, Job]]()
    for (mapElement <- cellState.simulator.schedulers.map(_._2).map(_.pastJobs)){
      pastJobsMaps = pastJobsMaps ++ mapElement
    }
    allPastTuples = allPastTuples ++ pastJobsMaps.map(_._2).toSeq
    allPastTuples = allPastTuples.sortBy(_._1)
    if(allPastTuples.length >= windowSize+1){
      allPastTuples = allPastTuples.slice(allPastTuples.length-(windowSize+2), allPastTuples.length-1)
    }
    for(i <- 1 to allPastTuples.length-1){
      interArrival = interArrival :+ (allPastTuples(i)._1 - allPastTuples(i-1)._1)
      memConsumed = memConsumed :+ allPastTuples(i)._2.numTasks*allPastTuples(i)._2.memPerTask
      cpuConsumed = cpuConsumed :+ allPastTuples(i)._2.numTasks*allPastTuples(i)._2.cpusPerTask
    }
    interArrivalAvg = interArrival.sum / interArrival.length
    memAvg = memConsumed.sum / memConsumed.length
    memDeviation = stddev(memConsumed)
    cpuAvg = cpuConsumed.sum / cpuConsumed.length
    cpuDeviation = stddev(cpuConsumed)
    if(interArrivalAvg > 0.0 && memAvg > 0.0 && cpuAvg > 0.0 && memDeviation > 0.0 && cpuDeviation > 0.0){
      val tdist = new ExponentialDistributionImpl(interArrivalAvg)
      val tprob = tdist.cumulativeProbability(ts)

      val ndistcpu = new NormalDistributionImpl(cpuAvg, cpuDeviation)
      val ncpuprob = ndistcpu.cumulativeProbability(cellState.availableCpus)

      val ndistmem = new NormalDistributionImpl(memAvg, memDeviation)
      val nmemprob = ndistcpu.cumulativeProbability(cellState.availableMem)
      val nprob = Math.min(nmemprob, ncpuprob) //Probabilidad de poder satisfacer. Cogemos el min para ser más restrictivos
      should = (1-nprob)/*Probabilidad de levantar*/*tprob /*Metemos un coeficiente que minimice la probabilidad en caso de no llegar tareas*/ <= threshold
    }
    should
  }

  def mean[T](item:Traversable[T])(implicit n:Numeric[T]) = {
    n.toDouble(item.sum) / item.size.toDouble
  }

  def variance[T](items:Traversable[T])(implicit n:Numeric[T]) : Double = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total,item)=>{
      val itemDbl = n.toDouble(item)
      val square = math.pow(itemDbl - itemMean,2)
      total + square
    })
    sumOfSquares / count.toDouble
  }

  def stddev[T](items:Traversable[T])(implicit n:Numeric[T]) : Double = {
    math.sqrt(variance(items))
  }

  override val name: String = "exponential-normal-power-off-decision"
}
