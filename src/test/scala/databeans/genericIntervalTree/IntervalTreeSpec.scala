package databeans.genericIntervalTree

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec

import java.util

/**
 * Specification test for depth.IntervalTree.
 *
 * @see [[databeans.genericIntervalTree.IntervalTree]]
 */
final class IntervalTreeSpec extends AnyFunSpec with Matchers {

  val NO_RESULT = new util.ArrayList()

  describe("Tree holding a single interval [1, 5]") {
    val intervals = Seq[Interval[Int]](Interval(1, 5, "file1"))
    val tree = IntervalTree(intervals)

    it("should not be empty") {
      tree.isEmpty shouldBe false
    }

    it("should return 1 result on query interval [4, 8]") {

      tree.get(Interval(4, 8, "file1")).size shouldEqual 1
      tree.getIntervals(Interval(4, 8, "file1")).size shouldEqual 1
    }
  }

  describe("depth.Node holding many intervals") {
    val intervals = Seq[Interval[Int]](
      Interval(1, 5, "file2"),
      Interval(6, 9, "file3"),
      Interval(10, 14, "file4")
    )

    val tree = IntervalTree(intervals)

    it("should not be empty") {
      tree.isEmpty shouldBe false
    }

    it("should return 2 results on query interval [6, 19]") {
      tree.get(Interval(6, 19, "file5")).size shouldEqual 2
      tree.getIntervals(Interval(6, 19, "file5")).size shouldEqual 2
    }
  }
}
