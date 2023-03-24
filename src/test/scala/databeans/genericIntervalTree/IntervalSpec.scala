package databeans.genericIntervalTree

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec


/**
 * Specification test for depth.Interval.
 */
final class IntervalSpec extends AnyFunSpec with Matchers {

  describe("An interval [1,5]") {
    val intInterval = Interval[Int] _
    val interval = intInterval(1, 5, "file1")


    it("should not intersect with [-1,0]") {
      interval.intersects(-1, 0) shouldBe false
    }

    it("should not intersect with [6,7]") {
      interval.intersects(6, 7) shouldBe false
    }

    it("should intersect with [0,1]") {
      interval.intersects(0, 1) shouldBe true
    }

    it("should intersect with [5,6]") {
      interval.intersects(5, 6) shouldBe true
    }

    it("should intersect with [2,3]") {
      interval.intersects(2, 3) shouldBe true
    }

    it("should intersect with [0,4]") {
      interval.intersects(0, 4) shouldBe true
    }

    it("should intersect with [3,10]") {
      interval.intersects(3, 10) shouldBe true
    }

    it("should intersect with [0,10]") {
      interval.intersects(0, 10) shouldBe true
    }


  }

  describe("An interval [b,c]") {
    val stringInterval = Interval[String] _
    val interval = stringInterval("b", "c", "file1")


    it("should not intersect with [d,e]") {
      interval.intersects("d", "e") shouldBe false
    }

    it("should intersect with [b,d]") {
      interval.intersects("b", "d") shouldBe true
    }
  }

  describe("An interval [b,c]") {
    val stringInterval = Interval[String] _
    val interval = stringInterval("b", "c", "file1")


    it("should be greater than [a,e]") {
      val interval2 = stringInterval("a", "e", "file2")
      interval.compareTo(interval2) shouldBe 1
    }

    it("should be lower than [f,g]") {
      val interval2 = stringInterval("f", "g", "file2")
      interval.compareTo(interval2) shouldBe -1
    }

    it("should be equal to [b,c]") {
      val interval2 = stringInterval("b", "c", "file2")
      interval.compareTo(interval2) shouldBe 0
    }
  }
}

