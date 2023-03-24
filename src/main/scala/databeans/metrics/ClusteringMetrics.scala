package databeans.metrics

import databeans.fileStatsIntervalTree
import databeans.fileStatsIntervalTree.{Interval, IntervalBoundary, IntervalTree}

import scala.collection.immutable.ListMap

case class ClusteringMetrics(
                              // column: String,
                              total_file_count: Long,
                              total_uniform_file_count: Long,
                              averageOverlapDepth: Double,
                              fileDepthHistogram: ListMap[Double, Int],
                              averageOverlaps: Double
                            )

// TODO Add file size histogram too as a metric to watch.
// TODO Add average file size metric, maybe min and max too?
// TODO Add the total table size
// TODO Add average files per partition and files number per partition histogram.
// TODO <Advanced> find columns correlated to insertion time to avoid clustering on those columns.
// TODO add skipping ratio as metric to watch.
object ClusteringMetrics {

  def computeMetrics(intervals: Seq[Interval]): ClusteringMetrics = {
    val representativePoints = intervals
      .flatMap(i => Seq(IntervalBoundary(i.start, i.statsType), IntervalBoundary(i.end, i.statsType)))
      .distinct
      .sorted
      .map(p => Interval(p.value, p.value, p.value, p.statsType))


    /*println("***************")
    representativePoints.foreach(println)
    println("****************")*/

    val tree = fileStatsIntervalTree.IntervalTree(intervals)
    var depthPerSubInterval: Seq[(Interval, Int)] = Seq()
    var histogramInput: Seq[(Interval, Int)] = Seq()
    var i = 0
    while (i < representativePoints.length) {
      val upperBoundOverlappingIntervals = tree.getIntervals(representativePoints(i))
      val upperBoundDepth = upperBoundOverlappingIntervals.size
      if (i > 0) {
        val interval = Interval(
          representativePoints(i - 1).start,
          representativePoints(i).end,
          s"]${representativePoints(i - 1).start},${representativePoints(i).end}[",
          representativePoints(i - 1).statsType
        )
        val overlappingIntervals = tree.getIntervals(interval, false)
        val openIntervalDepth = overlappingIntervals.size

        if (openIntervalDepth != depthPerSubInterval.last._2) {
          depthPerSubInterval = depthPerSubInterval ++ Seq((interval, openIntervalDepth))
        }

        if (upperBoundDepth != depthPerSubInterval.last._2) {
          depthPerSubInterval = depthPerSubInterval ++ Seq((representativePoints(i), upperBoundDepth))
        }
        histogramInput = histogramInput ++
          (upperBoundOverlappingIntervals ++ overlappingIntervals)
            .distinct
            .map(i => (i, Seq(depthPerSubInterval.last._2, openIntervalDepth, upperBoundDepth).max))

        i = i + 1
      }
      else {
        depthPerSubInterval = depthPerSubInterval ++ Seq((representativePoints(i), upperBoundDepth))
        histogramInput = histogramInput ++
          upperBoundOverlappingIntervals.map(i => (i, upperBoundDepth))
        i = i + 1
      }
    }

    // depthPerSubInterval.foreach(println)

    /*
    println("histogramInput:")
    histogramInput.foreach(println)
    println("**************")
    */

    val fileDepthHistogram = computeDepthHistogram(histogramInput)
    val averageOverlapDepth = computeAverageOverlapDepth(depthPerSubInterval)
    val averageOverlaps = computeAverageOverlaps(intervals)
    val uniformFilesCount = countUniformFiles(intervals)


    ClusteringMetrics(intervals.size.toLong, uniformFilesCount, averageOverlapDepth, fileDepthHistogram, averageOverlaps)
  }

  def computeMetricsV0(intervals: Seq[Interval]): ClusteringMetrics = {
    var sortedIntervals = intervals.sorted

    var i = 0
    var fixedInterval = sortedIntervals.head
    val length = sortedIntervals.length
    var depthPerSubInterval: Seq[(Interval, Int)] = Seq()
    var histogramInput: Seq[(Interval, Int)] = Seq()
    var isUniform = false
    var break = false
    while (i < length && !break) {

      val tree = fileStatsIntervalTree.IntervalTree(sortedIntervals)
      val candidates = if (fixedInterval.start != fixedInterval.end) {
        isUniform = false
        tree.getIntervals(fixedInterval, false)
      } else {
        isUniform = true
        tree.getIntervals(fixedInterval)
      }


      /*println(s"candidates for $fixedInterval")
      candidates.foreach(println)*/
      // println("*******************")

      if (candidates.nonEmpty) {
        // extract overlaps
        val overlaps = candidates
          .map { c => getOverlap(c, fixedInterval) }.distinct

        /*println("overlaps")
        overlaps.foreach(println)*/
        // println("*******************")

        // build sub intervals from overlaps.
        val subIntervals = buildSubIntervals(overlaps)

        /*println("subIntervals")
        subIntervals.foreach(println)
        println("*******************")*/


        var depthPerOverlap = computeDepthPerSubInterval(subIntervals, candidates, isUniform)
        depthPerSubInterval = depthPerSubInterval ++ depthPerOverlap

        val max = subIntervals.max.end
        val uniformAtMax = sortedIntervals.filter(i => i.start == i.end & i.start == max)

        var allIntervals = candidates
        if (uniformAtMax.nonEmpty & fixedInterval.start != fixedInterval.end) {
          val depthOfUniformInterval = computeDepthPerSubInterval(Seq(uniformAtMax.head), candidates ++ uniformAtMax, true)
          depthPerOverlap = depthPerOverlap ++ depthOfUniformInterval
          depthPerSubInterval = depthPerSubInterval ++ depthOfUniformInterval
          allIntervals = allIntervals ++ uniformAtMax
        }

        // compute histogram
        val max_depth = depthPerOverlap.map(_._2).max
        histogramInput = histogramInput ++ allIntervals.map(i => (i, max_depth))

        /*  println("histogramInput")
          histogramInput.foreach(println)
          println("*******************")*/

        // build the next interval.

        sortedIntervals = sortedIntervals.filter(i => i.endsAfter(max))

        if (sortedIntervals.nonEmpty) {
          val upperBoundCandidates = sortedIntervals
            .flatMap(i => Seq(IntervalBoundary(i.start, i.statsType), IntervalBoundary(i.end, i.statsType)))
            .filter(_.greaterThen(fixedInterval.end))

          val lowerBound = fixedInterval.end
          val upperBound = upperBoundCandidates.min
          fixedInterval = Interval(lowerBound, upperBound.value, s"[$lowerBound,$upperBound]", upperBound.statsType)
          i = i + 1
        }
        else {
          break = true
        }


      }
      else {
        if (sortedIntervals.nonEmpty) {
          fixedInterval = sortedIntervals.head
        }
        else {
          break = true
        }
      }

      /*println("sortedIntervals")
      sortedIntervals.foreach(println)
      println("***********************")*/

      /* println("***********************")
       println("next fixed interval")
       println(fixedInterval)*/

    }


    /* println("depth Per SubInterval")
     depthPerSubInterval.foreach(println)
     println("*********************")

     println("histogramInput")
     histogramInput.foreach(println)
     println("********************")*/

    val fileDepthHistogram = computeDepthHistogram(histogramInput)
    val averageOverlapDepth = computeAverageOverlapDepth(depthPerSubInterval)
    val averageOverlaps = computeAverageOverlaps(intervals)
    val uniformFilesCount = countUniformFiles(intervals)


    ClusteringMetrics(intervals.size.toLong, uniformFilesCount, averageOverlapDepth, fileDepthHistogram, averageOverlaps)
  }

  def removeElement(list: Seq[Interval], fixedInterval: Interval): Seq[Interval] = {
    list.filter(_.end != fixedInterval.start)
  }

  def getOverlap(i1: Interval, i2: Interval): Interval = {
    val sorted = Seq(i1, i2).flatMap { i =>
      Seq(IntervalBoundary(i.start, i.statsType), IntervalBoundary(i.end, i.statsType))
    }.sorted

    val start = sorted(1).value
    val end = sorted(2).value
    Interval(start, end, s"[$start,$end]", i1.statsType)
  }

  def buildSubIntervals(overlaps: Seq[Interval]) = {
    val elements = overlaps.flatMap(i =>
      Seq(IntervalBoundary(i.start, i.statsType),
        IntervalBoundary(i.end, i.statsType))
    ).distinct.sorted

    var i = 0
    var extractedIntervals: Seq[Interval] = Seq()
    val statsType = elements.head.statsType
    while (i < elements.length - 1) {
      val lowerBound = elements(i).value
      val upperBound = elements(i + 1).value
      extractedIntervals = extractedIntervals ++
        Seq(Interval(lowerBound, upperBound, s"[$lowerBound, $upperBound]", statsType))
      i = i + 1
    }

    if (extractedIntervals.nonEmpty) {
      extractedIntervals
    }
    else {
      overlaps
    }

  }

  def computeDepthPerSubInterval(subIntervals: Seq[Interval], candidates: Seq[Interval], isUniform: Boolean): Seq[(Interval, Int)] = {
    val subTree = fileStatsIntervalTree.IntervalTree(candidates)

    if (isUniform) {
      subIntervals.map(i => (i, subTree.getIntervals(i).size))
    } else {
      subIntervals.map(i => (i, subTree.getIntervals(i, false).size))
    }
  }

  def computeAverageOverlapDepth(depthPerSubInterval: Seq[(Interval, Int)]): Double = {
    val depths = depthPerSubInterval.filter(_._2 > 1)
    if (depths.nonEmpty) {
      "%.4f".format(depths.map(_._2).sum.toFloat / depthPerSubInterval.count(_._2 > 1)).toDouble
    }
    else {
      1.0
    }
  }

  def computeDepthHistogram(histogramInput: Seq[(Interval, Int)]): ListMap[Double, Int] = {
    val data = histogramInput.groupBy(_._1).values.map(_.maxBy(_._2)).map(_._2.toDouble).toList
    Distribution.histogram(data)
  }

  def computeAverageOverlaps(intervals: Seq[Interval]): Double = {
    val tree = fileStatsIntervalTree.IntervalTree(intervals)
    val intervalsOverlaps = intervals
      .map(i => tree.getIntervals(i).size - 1)

    "%.4f".format(intervalsOverlaps.sum.toFloat / intervalsOverlaps.size).toDouble
  }

  def countUniformFiles(intervals: Seq[Interval]): Int = {
    intervals.count(i => i.start == i.end)
  }

  def computeFileSizeHistogram(sizes: List[Double]) = {
    Distribution.histogram(sizes)
  }

}



