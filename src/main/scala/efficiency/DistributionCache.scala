package efficiency

import ClusterSchedulingSimulation.Job

import scala.collection.immutable.Queue

/**
  * Created by dfernandez on 18/2/16.
  */
object DistributionCache {


  val queueSize = 25
  // Key: Mean, Deviation, Threshold -> Value: inverseCumulativeProbability
  var normalDistributionCache =  scala.collection.mutable.Map[Tuple3[Double, Double, Double], Double]()
  var normalDistributionCacheCalls = 0
  var normalDistributionCacheMiss = 0
  def normalDistributionCacheHits = normalDistributionCacheCalls - normalDistributionCacheMiss
  // Key: Alpha, Beta, Threshold -> Value: cumulativeProbability(Threshold)
  var gammaDistributionCache =  scala.collection.mutable.Map[Tuple3[Double, Double, Double], Double]()
  var gammaDistributionCacheCalls = 0
  var gammaDistributionCacheMiss = 0
  def gammaDistributionCacheHits = gammaDistributionCacheCalls - gammaDistributionCacheMiss
  // Key: List[Long], Tuple6[Double] 1- Interarrival avg, 2- Interarrival stddev, 3 - mem avg, 4 - mem stddev
  // 5 - cpu avg, 6- cpu stddev
  var jobAttributesCache =  scala.collection.mutable.Map[Seq[Long], Tuple6[Double, Double, Double, Double, Double, Double]]()
  var jobAttributesCacheCalls = 0
  var jobAttributesCacheMiss = 0
  def jobAttributesCacheHits = jobAttributesCacheCalls - jobAttributesCacheMiss
  var jobCache = Seq[Tuple2[Double, Job]]()

  def addCacheJob(tuple : Tuple2[Double, Job]): Unit = {
    if(jobCache.size > 0)
      assert(tuple._1 > jobCache.last._1, "Intentando añadir un job a la cache cuyo tiempo de inicio es anterior al ultimo encontrado")
    jobCache = jobCache :+ tuple
    if(jobCache.size > queueSize+1){
      jobCache = jobCache.drop(1)
    }
  }

}
