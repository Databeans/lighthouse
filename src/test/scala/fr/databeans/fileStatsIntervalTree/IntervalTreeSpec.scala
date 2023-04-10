package fr.databeans.fileStatsIntervalTree

import org.apache.spark.sql.types.IntegerType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.util


final class IntervalTreeSpec extends AnyFunSpec with Matchers {

  val NO_RESULT = new util.ArrayList()

  describe("Tree holding a single interval [1, 5]") {
    val intervals = Seq[Interval](Interval("1", "5", "file1", IntegerType))
    val tree = IntervalTree(intervals)

    it("should not be empty") {
      tree.isEmpty shouldBe false
    }

    it("should return 1 result on query interval [4, 8]") {
      tree.get(Interval("4", "8", "file1", IntegerType)).size shouldEqual 1
      tree.getIntervals(Interval("4", "8", "file1", IntegerType)).size shouldEqual 1
    }
  }

  describe("depth.Node holding many intervals") {
    val intervals = Seq[Interval](
      Interval("1", "5", "file2", IntegerType),
      Interval("6", "9", "file3", IntegerType),
      Interval("10", "14", "file4", IntegerType)
    )

    val tree = IntervalTree(intervals)

    it("should not be empty") {
      tree.isEmpty shouldBe false
    }

    it("should return 2 results on query interval [6, 19]") {
      tree.get(Interval("6", "19", "file5", IntegerType)).size shouldEqual 2
      tree.getIntervals(Interval("6", "19", "file5", IntegerType)).size shouldEqual 2
    }
  }
}
