package efficiency

import java.util.Collections


/**
  * Created by dfernandez on 18/2/16.
  */
object DistributionCache {

  // Key: Mean, Deviation, Threshold -> Value: inverseCumulativeProbability
  var normalDistributionCache =  Collections.synchronizedMap(new LFUCache[Tuple3[Double, Double, Double], Double](1500, 0.3f))
  var normalDistributionCacheCalls = 0
  var normalDistributionCacheMiss = 0
  def normalDistributionCacheHits = normalDistributionCacheCalls - normalDistributionCacheMiss
  // Key: Alpha, Beta, Threshold -> Value: cumulativeProbability(Threshold)
  var gammaDistributionCache =  Collections.synchronizedMap(new LFUCache[Tuple3[Double, Double, Double], Double](15000, 0.3f))
  var gammaDistributionCacheCalls = 0
  var gammaDistributionCacheMiss = 0
  def gammaDistributionCacheHits = gammaDistributionCacheCalls - gammaDistributionCacheMiss
  // Key: List[Long], Tuple6[Double] 1- Interarrival avg, 2- Interarrival stddev, 3 - mem avg, 4 - mem stddev
  // 5 - cpu avg, 6- cpu stddev
  var jobAttributesCache = Collections.synchronizedMap(new LFUCache[Seq[Long], Tuple6[Double, Double, Double, Double, Double, Double]](1500, 0.3f))
  var jobAttributesCacheCalls = 0
  var jobAttributesCacheMiss = 0
  def jobAttributesCacheHits = jobAttributesCacheCalls - jobAttributesCacheMiss


}
