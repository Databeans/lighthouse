package databeans.metrics

import scala.collection.immutable.ListMap

object Distribution {

  def roundToPowerOfTwo(element: Int): Double = {
    val log = Math.log(element) / Math.log(2);
    val roundLog = Math.round(log);
    val powerOfTwo = Math.pow(2, roundLog);
    if (powerOfTwo > element) {
      Math.pow(2, roundLog - 1)
    }
    else {
      Math.pow(2, roundLog)
    }
  }

  def getBounds(maxBin: Double): List[Double] = {
    var powerOfTwo = 32
    var bins: List[Int] = List.range(1, 17)
    while (powerOfTwo < maxBin) {
      bins = bins ++ Seq(powerOfTwo)
      powerOfTwo = powerOfTwo * 2
    }
    bins.map(_.toDouble)
  }

  def computePopulatedBuckets(data: List[Double]): Map[Double, Int] = {
    data.map(_.floor.toInt).map { e =>
      if (e > 16)
        roundToPowerOfTwo(e)
      else e
    }.groupBy(identity).mapValues(_.size)
  }

  def computeUnPopulatedBuckets(maxBin: Double, populatedBuckets: Map[Double, Int]): Map[Double, Int] = {
    getBounds(maxBin).map(e => (e, 0)).toMap.filter(x => !populatedBuckets.keys.toList.contains(x._1))
  }

  def histogram(data: List[Double]): Map[Double, Int] = {
    val maxBin = data.max
    val populatedBuckets = computePopulatedBuckets(data)
    val unPopulatedBuckets = computeUnPopulatedBuckets(maxBin, populatedBuckets)
    Map((populatedBuckets ++ unPopulatedBuckets).toSeq.sortBy(_._1): _*)
  }
}

