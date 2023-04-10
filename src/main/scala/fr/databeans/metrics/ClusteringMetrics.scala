package fr.databeans.metrics

import fr.databeans.fileStatsIntervalTree
import fr.databeans.fileStatsIntervalTree.{Interval, IntervalBoundary, IntervalTree}

case class ClusteringMetrics(
                              column: String,
                              total_file_count: Long,
                              total_uniform_file_count: Long,
                              averageOverlapDepth: Double,
                              fileDepthHistogram: Map[Double, Int],
                              averageOverlaps: Double
                            )


class ClusteringMetricsBuilder {

  def computeMetrics(column: String, intervals: Seq[Interval]): ClusteringMetrics = {

    val uniformFilesCount = countUniformFiles(intervals)

    val filteredIntervals = intervals.filter(i => i.start != null & i.end != null)

    if (filteredIntervals.nonEmpty) {
      val representativePoints = filteredIntervals
        .flatMap(i => Seq(IntervalBoundary(i.start, i.statsType), IntervalBoundary(i.end, i.statsType)))
        .distinct
        .sorted
        .map(p => Interval(p.value, p.value, p.value, p.statsType))

      val tree = IntervalTree(filteredIntervals)
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

      val fileDepthHistogram = computeDepthHistogram(histogramInput)
      val averageOverlapDepth = computeAverageOverlapDepth(depthPerSubInterval)
      val averageOverlaps = computeAverageOverlaps(filteredIntervals)

      ClusteringMetrics(column, intervals.size.toLong, uniformFilesCount, averageOverlapDepth, fileDepthHistogram, averageOverlaps)
    }
    else {
      ClusteringMetrics(column, intervals.size.toLong, uniformFilesCount, -1.toDouble, null.asInstanceOf[Map[Double, Int]], -1.toDouble)
    }
  }

  private def computeAverageOverlapDepth(depthPerSubInterval: Seq[(Interval, Int)]): Double = {
    val depths = depthPerSubInterval.filter(_._2 > 1)
    if (depths.nonEmpty) {
      "%.4f".format(depths.map(_._2).sum.toFloat / depthPerSubInterval.count(_._2 > 1)).toDouble
    }
    else {
      1.0
    }
  }

  private def computeDepthHistogram(histogramInput: Seq[(Interval, Int)]): Map[Double, Int] = {
    val data = histogramInput.groupBy(_._1).values.map(_.maxBy(_._2)).map(_._2.toDouble).toList
    Distribution.histogram(data)
  }

  private def computeAverageOverlaps(intervals: Seq[Interval]): Double = {
    val tree = fileStatsIntervalTree.IntervalTree(intervals)
    val intervalsOverlaps = intervals
      .map(i => tree.getIntervals(i).size - 1)

    "%.4f".format(intervalsOverlaps.sum.toFloat / intervalsOverlaps.size).toDouble
  }

  private def countUniformFiles(intervals: Seq[Interval]): Int = {
    intervals.count(i => i.start == i.end)
  }
}



