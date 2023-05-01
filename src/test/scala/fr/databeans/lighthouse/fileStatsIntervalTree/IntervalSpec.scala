package fr.databeans.lighthouse.fileStatsIntervalTree

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.types._

final class IntervalSpec extends AnyFunSpec with Matchers {

  describe("An interval [b,c]") {

    val interval = Interval("b", "c", "file1", StringType)

    it("should not intersect with [d,e]") {
      interval.intersects("d", "e") shouldBe false
    }

    it("should intersect with [b,d]") {
      interval.intersects("b", "d") shouldBe true
    }
  }

  describe("An interval [b,c]") {

    val interval = Interval("b", "c", "file1", StringType)

    it("should be greater than [a,e]") {
      val interval2 = Interval("a", "e", "file2", StringType)
      interval.compareTo(interval2) shouldBe 1
    }

    it("should be lower than [f,g]") {
      val interval2 = Interval("f", "g", "file2", StringType)
      interval.compareTo(interval2) shouldBe -1
    }

    it("should be equal to [b,c]") {
      val interval2 = Interval("b", "c", "file2", StringType)
      interval.compareTo(interval2) shouldBe 0
    }
  }

  describe("An interval [1,5]") {

    val interval = Interval("1", "5", "file1", IntegerType)

    it("should not intersect with [-1,0]") {
      interval.intersects("-1", "0") shouldBe false
    }

    it("should not intersect with [6,17]") {
      interval.intersects("6", "17") shouldBe false
    }

    it("should intersect with [0,1]") {
      interval.intersects("0", "1") shouldBe true
    }

    it("should intersect with [5,6]") {
      interval.intersects("5", "6") shouldBe true
    }

    it("should intersect with [2,3]") {
      interval.intersects("2", "3") shouldBe true
    }

    it("should intersect with [0,4]") {
      interval.intersects("0", "4") shouldBe true
    }

    it("should intersect with [3,10]") {
      interval.intersects("3", "10") shouldBe true
    }

    it("should intersect with [0,10]") {
      interval.intersects("0", "10") shouldBe true
    }
  }

  describe("An interval [7,17]") {
    val interval = Interval("7", "17", "file1", IntegerType)


    it("should intersect with [5,20]") {
      interval.intersects("5", "20") shouldBe true
    }
  }
}

