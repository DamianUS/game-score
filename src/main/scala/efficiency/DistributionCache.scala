package efficiency

import ClusterSchedulingSimulation.Job

import scala.collection.immutable.Queue

/**
  * Created by dfernandez on 18/2/16.
  */
object DistributionCache {

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


}
