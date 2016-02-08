package efficiency.power_off_policies.decision.probabilistic

import ClusterSchedulingSimulation.{CellState, Job}
import efficiency.power_off_policies.decision.PowerOffDecision
import org.apache.commons.math.distribution.{NormalDistributionImpl, GammaDistributionImpl}

/**
 * Created by dfernandez on 22/1/16.
 */
class GammaNormalPowerOffDecision(normalThreshold: Double, threshold : Double, windowSize: Int) extends PowerOffDecision{
  override def shouldPowerOff(cellState: CellState, machineID: Int): Boolean = {
    //FIXME: Esto no calcula bien
    //TODO: Calculate Ts
    val ts = 130.0
    var should = false
    var interArrivalAvg = 0.0
    var memAvg = 0.0
    var cpuAvg = 0.0
    var memDeviation = 0.0
    var cpuDeviation = 0.0
    var allPastTuples = Seq[Tuple3[Double, Job, Boolean]]()
    var interArrival = Seq[Double]()
    var memConsumed = Seq[Double]()
    var cpuConsumed = Seq[Double]()
    cellState.simulator.schedulers.map(_._2).foreach(_.cleanPastJobs(windowSize+1))
    var pastJobsMaps = Map[Long, Tuple3[Double, Job, Boolean]]()
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
      val alphaCpu = cellState.availableCpus / new NormalDistributionImpl(cpuAvg, cpuDeviation).inverseCumulativeProbability(normalThreshold)
      val alphaMem = cellState.availableMem / new NormalDistributionImpl(memAvg, memDeviation).inverseCumulativeProbability(normalThreshold)
      //FIXME: en la implementación anterior teníamos un floor de (alphacpu+alphamem) /2 y le sumábamos 1
      val dist = new GammaDistributionImpl( Math.min(alphaCpu,alphaMem), interArrivalAvg)
      val prob = dist.cumulativeProbability(ts)
      should = prob <= threshold
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

  override val name: String = "gamma-normal-power-off-decision"
}
