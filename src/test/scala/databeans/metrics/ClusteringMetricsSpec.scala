package databeans.metrics

import databeans.fileStatsIntervalTree.{Interval, IntervalBoundary, Node}
import databeans.fileStatsIntervalTree
import databeans.metrics.ClusteringMetrics.{computeAverageOverlapDepth, computeDepthHistogram}
import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap


class ClusteringMetricsSpec extends AnyFunSpec with Matchers {

  //TODO Use the compute unpopulated function from the distribution instead
  def buildHistogram(maxBin: Int, populatedBuckets: Map[Double, Int]): Map[Double, Int] = {
    val missingBins = Distribution.computeUnPopulatedBuckets(maxBin, populatedBuckets)
    missingBins ++ populatedBuckets
  }

  describe("compute the overlap metrics") {

    it("should return 2.0 as overlap depth") {

      val intervals1 = Seq[Interval](
        Interval("1", "2", "file2", IntegerType),
        Interval("3", "4", "file3", IntegerType),
        Interval("0", "5", "file5", IntegerType),
        Interval("4", "10", "file6", IntegerType),
        Interval("14", "15", "file8", IntegerType),
        Interval("14", "20", "file9", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals1)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps

      avgOverlapDepth shouldBe 2.2

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((2.0, 3), (3.0, 3)))

      averageOverlaps shouldBe 1.6667
    }

    it("should return the number of files when all files have the same min max") {

      val intervals2 = Seq[Interval](
        Interval("1", "2", "file2", IntegerType),
        Interval("1", "2", "file3", IntegerType),
        Interval("1", "2", "file5", IntegerType),
        Interval("1", "2", "file6", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals2)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps

      avgOverlapDepth shouldBe 4.0000

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((4.0, 4)))

      averageOverlaps shouldBe 3.0
    }

    it("should return 3.7778 as overlap depth and compute the histogram") {

      val intervals3 = Seq[Interval](
        Interval("1", "5", "file2", IntegerType),
        Interval("0", "7", "file3", IntegerType),
        Interval("11", "16", "file4", IntegerType),
        Interval("7", "16", "file5", IntegerType),
        Interval("5", "9", "file6", IntegerType),
        Interval("4", "16", "file7", IntegerType),
        Interval("0", "13", "file8", IntegerType),
        Interval("9", "12", "file9", IntegerType),
        Interval("7", "9", "file10", IntegerType),
        Interval("20", "30", "file11", IntegerType),
        Interval("31", "40", "file12", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals3)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 4.25

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((1.0, 2), (5.0, 2), (6.0, 7)))

      averageOverlaps shouldBe 5.0909

      total_file_count shouldBe 11

      total_uniform_file_count shouldBe 0
    }

    it("should return 1 as overlap depth and compute the histogram") {

      val intervals4 = Seq[Interval](
        Interval("1", "2", "file2", IntegerType),
        Interval("3", "4", "file3", IntegerType),
        Interval("5", "6", "file4", IntegerType),
        Interval("7", "8", "file5", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals4)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 1.0

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((1.0, 4)))

      averageOverlaps shouldBe 0

      total_file_count shouldBe 4

      total_uniform_file_count shouldBe 0
    }

    it("BUG: min = max for all intervals") {

      val intervals5 = Seq[Interval](
        Interval("1", "1", "file2", IntegerType),
        Interval("1", "1", "file3", IntegerType),
        Interval("1", "1", "file5", IntegerType),
        Interval("1", "1", "file6", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals5)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 4.0000

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((4.0, 4)))

      averageOverlaps shouldBe 3.0

      total_file_count shouldBe 4

      total_uniform_file_count shouldBe 4
    }

    it("example 2") {

      val intervals2 = Seq[Interval](
        Interval("1", "4", "file2", IntegerType),
        Interval("2", "6", "file3", IntegerType),
        Interval("5", "7", "file5", IntegerType),
        Interval("5", "10", "file6", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals2)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 2.3333

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((2.0, 1), (3.0, 3)))

      averageOverlaps shouldBe 2.0

      total_file_count shouldBe 4

      total_uniform_file_count shouldBe 0
    }

    it("example 3") {

      val intervals2 = Seq[Interval](
        Interval("1", "2", "file2", IntegerType),
        Interval("3", "5", "file3", IntegerType),
        Interval("3", "5", "file5", IntegerType),
        Interval("3", "5", "file6", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals2)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 3

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((1.0, 1), (3.0, 3)))

      averageOverlaps shouldBe 1.5

      total_file_count shouldBe 4

      total_uniform_file_count shouldBe 0
    }

    it("example 4") {

      val intervals2 = Seq[Interval](
        Interval("1", "2", "file2", IntegerType),
        Interval("3", "5", "file3", IntegerType),
        Interval("4", "7", "file5", IntegerType),
        Interval("6", "8", "file6", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals2)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 2

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((1.0, 1), (2.0, 3)))

      averageOverlaps shouldBe 1.0

      total_file_count shouldBe 4

      total_uniform_file_count shouldBe 0
    }

    it("intervals have one uniform interval") {

      val intervals = Seq[Interval](
        Interval("1", "5", "file2", IntegerType),
        Interval("4", "8", "file3", IntegerType),
        Interval("6", "9", "file5", IntegerType),
        Interval("7", "7", "file6", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 2.25

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((2.0, 1), (3.0, 3)))

      averageOverlaps shouldBe 2.0

      total_file_count shouldBe 4

      total_uniform_file_count shouldBe 1
    }

    it("intervals have two uniform intervals") {

      val intervals = Seq[Interval](
        Interval("1", "5", "file2", IntegerType),
        Interval("4", "8", "file3", IntegerType),
        Interval("6", "9", "file5", IntegerType),
        Interval("7", "7", "file6", IntegerType),
        Interval("7", "7", "file7", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 2.5

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((2.0, 1), (4.0, 4)))

      averageOverlaps shouldBe 2.8

      total_file_count shouldBe 5

      total_uniform_file_count shouldBe 2
    }

    it("intervals start with uniform interval and have gaps") {

      val intervals = Seq[Interval](
        Interval("0", "0", "file1", IntegerType),
        Interval("0", "0", "file2", IntegerType),
        Interval("1", "5", "file2", IntegerType),
        Interval("4", "8", "file3", IntegerType),
        Interval("6", "9", "file5", IntegerType),
        Interval("7", "7", "file6", IntegerType),
        Interval("7", "7", "file7", IntegerType)
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 2.4

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((2.0, 3), (4.0, 4)))

      averageOverlaps shouldBe 2.2857

      total_file_count shouldBe 7

      total_uniform_file_count shouldBe 4
    }


  }

  describe("decimal type") {
    it("BUG: decimal type should be supported for statistics") {

      val intervals = Seq[Interval](
        Interval("-8.00", "-5.00", "file1", DecimalType(5, 2))
      )

      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram
      val averageOverlaps = overlapMetrics.averageOverlaps
      val total_file_count = overlapMetrics.total_file_count
      val total_uniform_file_count = overlapMetrics.total_uniform_file_count

      avgOverlapDepth shouldBe 1.0

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((1.0, 1)))

      averageOverlaps shouldBe 0

      total_file_count shouldBe 1

      total_uniform_file_count shouldBe 0
    }
  }


  describe("Node holding many intervals all in one group") {
    val intervals = Seq[Interval](
      Interval("16", "32", "file2", IntegerType),
      Interval("4", "40", "file3", IntegerType),
      Interval("10", "38", "file3", IntegerType),
      Interval("2", "24", "file5", IntegerType),
      Interval("6", "28", "file6", IntegerType)
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    it("should return 5 as overlap depth") {
      val overlapMetrics = ClusteringMetrics.computeMetrics(intervals)
      val avgOverlapDepth = overlapMetrics.averageOverlapDepth
      val overlapDepthHistogram = overlapMetrics.fileDepthHistogram

      avgOverlapDepth shouldBe 3.2857

      overlapDepthHistogram shouldBe
        buildHistogram(16, Map((5.0, 5)))
    }
  }


  ignore("optimized depth algorithm V2") {


    def computeDepth(intervals2: Seq[Interval]) = {
      val representativePoints = intervals2
        .flatMap(i => Seq(IntervalBoundary(i.start, i.statsType), IntervalBoundary(i.end, i.statsType)))
        .distinct
        .sorted
        .map(p => Interval(p.value, p.value, p.value, p.statsType))


      /*println("***************")
      representativePoints.foreach(println)
      println("****************")*/

      val tree = fileStatsIntervalTree.IntervalTree(intervals2)
      val depthPerPoint = representativePoints.map(p => (p, tree.getIntervals(p).size))


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
          i = i + 1
        }
      }

      // depthPerSubInterval.foreach(println)

      println("histogramInput:")
      histogramInput.foreach(println)
      println("**************")

      (depthPerSubInterval, histogramInput)
    }

    val intervals0 = Seq[Interval](
      Interval("1", "2", "file2", IntegerType),
      Interval("3", "4", "file3", IntegerType),
      Interval("0", "5", "file5", IntegerType),
      Interval("4", "10", "file6", IntegerType),
      Interval("14", "15", "file8", IntegerType),
      Interval("14", "20", "file9", IntegerType)
    )
    // avgOverlapDepth shouldBe 2.0

    val intervals1 = Seq[Interval](
      Interval("1", "2", "file2", IntegerType),
      Interval("1", "2", "file3", IntegerType),
      Interval("1", "2", "file5", IntegerType),
      Interval("1", "2", "file6", IntegerType)
    )
    // avgOverlapDepth shouldBe 4.0000

    val intervals2 = Seq[Interval](
      Interval("1", "5", "file2", IntegerType),
      Interval("0", "7", "file3", IntegerType),
      Interval("11", "16", "file4", IntegerType),
      Interval("7", "16", "file5", IntegerType),
      Interval("5", "9", "file6", IntegerType),
      Interval("4", "16", "file7", IntegerType),
      Interval("0", "13", "file8", IntegerType),
      Interval("9", "12", "file9", IntegerType),
      Interval("7", "9", "file10", IntegerType),
      Interval("20", "30", "file11", IntegerType),
      Interval("31", "40", "file12", IntegerType)
    )
    // avgOverlapDepth shouldBe 3.7778

    val intervals3 = Seq[Interval](
      Interval("1", "2", "file2", IntegerType),
      Interval("3", "4", "file3", IntegerType),
      Interval("5", "6", "file4", IntegerType),
      Interval("7", "8", "file5", IntegerType)
    )
    // avgOverlapDepth shouldBe 1.0

    val intervals4 = Seq[Interval](
      Interval("1", "1", "file2", IntegerType),
      Interval("1", "1", "file3", IntegerType),
      Interval("1", "1", "file5", IntegerType),
      Interval("1", "1", "file6", IntegerType)
    )
    // avgOverlapDepth shouldBe 4.0000

    val intervals5 = Seq[Interval](
      Interval("0", "6", "file2", IntegerType),
      Interval("1", "3", "file3", IntegerType),
      Interval("3", "5", "file5", IntegerType),
      Interval("5", "10", "file6", IntegerType)
    )

    val intervals6 = Seq[Interval](
      Interval("0", "2", "file2", IntegerType),
      Interval("0", "1", "file3", IntegerType),
      Interval("1", "4", "file5", IntegerType)
    )


    it("run all tests") {

      // good
      computeAverageOverlapDepth(computeDepth(intervals0)._1) shouldBe 2.2
      println(computeDepthHistogram(computeDepth(intervals1)._2))


      computeAverageOverlapDepth(computeDepth(intervals1)._1) shouldBe 4.0000
      computeAverageOverlapDepth(computeDepth(intervals2)._1) shouldBe 4.25
      computeAverageOverlapDepth(computeDepth(intervals3)._1) shouldBe 1.0
      computeAverageOverlapDepth(computeDepth(intervals4)._1) shouldBe 4.0000
      computeAverageOverlapDepth(computeDepth(intervals5)._1) shouldBe 2.4
      computeAverageOverlapDepth(computeDepth(intervals6)._1) shouldBe 2.3333



    }
  }


}
