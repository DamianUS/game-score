package efficiency

import ClusterSchedulingSimulation.{Job, CellState}
import org.apache.commons.math.distribution.{NormalDistributionImpl, GammaDistributionImpl}

/**
  * Created by dfernandez on 18/2/16.
  */
trait GammaUtils {
  def getGammaDistributionCummulativeProbability(alpha: Double, beta: Double, ts: Double): Double ={
    DistributionCache.gammaDistributionCacheCalls += 1
    DistributionCache.gammaDistributionCache.getOrElseUpdate((alpha, beta, ts), generateGammaDistributionCummulativeProbability(alpha, beta, ts))
  }

  def generateGammaDistributionCummulativeProbability(alpha: Double, beta: Double, ts: Double): Double ={
    DistributionCache.gammaDistributionCacheMiss += 1
    new GammaDistributionImpl(alpha, beta).cumulativeProbability(ts)
  }


  def getPastTuples(cellState: CellState, windowSize: Int): Seq[Tuple2[Double, Job]] ={
    /*var allPastTuples = Seq[Tuple2[Double, Job]]()
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
    allPastTuples*/
    val jobCacheLength = cellState.simulator.jobCache.length
    if(jobCacheLength > windowSize+1){
      cellState.simulator.jobCache.slice(jobCacheLength-(windowSize+1), jobCacheLength)
    }
    else{
      cellState.simulator.jobCache
    }
  }

  def getJobAttributes(pastTuples : Seq[Tuple2[Double, Job]]): Tuple6[Double, Double, Double, Double, Double, Double] ={
    DistributionCache.jobAttributesCacheCalls += 1
    DistributionCache.jobAttributesCache.getOrElseUpdate(pastTuples.map(_._2).map(_.id),generateJobAtributes(pastTuples))
  }

  def generateJobAtributes(allPastTuples : Seq[Tuple2[Double, Job]]): Tuple6[Double, Double, Double, Double, Double, Double] ={
    val allPastTuplesLength = allPastTuples.length
    DistributionCache.jobAttributesCacheMiss += 1
    val arraySize = if (allPastTuplesLength > 0) allPastTuplesLength-1 else 0
    val interArrival = new Array[Double](arraySize)
    val memConsumed = new Array[Double](arraySize)
    val cpuConsumed = new Array[Double](arraySize)
    for(i <- 1 to allPastTuplesLength-1){
      interArrival(i-1) = (allPastTuples(i)._1 - allPastTuples(i-1)._1)
      memConsumed(i-1) = allPastTuples(i)._2.numTasks*allPastTuples(i)._2.memPerTask
      cpuConsumed(i-1) = allPastTuples(i)._2.numTasks*allPastTuples(i)._2.cpusPerTask
    }
    val interArrivalTuple = meanAndStdDev(interArrival)
    val memTuple = meanAndStdDev(memConsumed)
    val cpuTuple = meanAndStdDev(cpuConsumed)
    (interArrivalTuple._1, interArrivalTuple._2, memTuple._1, memTuple._2, cpuTuple._1, cpuTuple._2)
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

  def meanAndStdDev[T](items:Traversable[T])(implicit n:Numeric[T]) : Tuple2[Double, Double] = {
    val itemMean = mean(items)
    val count = items.size
    val sumOfSquares = items.foldLeft(0.0d)((total,item)=>{
      val itemDbl = n.toDouble(item)
      val square = math.pow(itemDbl - itemMean,2)
      total + square
    })
    val variance = sumOfSquares / count.toDouble
    val stddev = math.sqrt(variance)
    (itemMean, stddev)
  }

  def getNormalDistributionInverseCummulativeProbability(normalAvg: Double, normalStdDev: Double, normalThreshold: Double): Double ={
    DistributionCache.normalDistributionCacheCalls += 1
    DistributionCache.normalDistributionCache.getOrElseUpdate((normalAvg, normalStdDev, normalThreshold), generateNormalDistributionInverseCummulativeProbability(normalAvg, normalStdDev, normalThreshold))
  }

  def generateNormalDistributionInverseCummulativeProbability(normalAvg: Double, normalStdDev: Double, normalThreshold: Double): Double ={
    DistributionCache.normalDistributionCacheMiss += 1
    new NormalDistributionImpl(normalAvg, normalStdDev).inverseCumulativeProbability(normalThreshold)
  }
}
