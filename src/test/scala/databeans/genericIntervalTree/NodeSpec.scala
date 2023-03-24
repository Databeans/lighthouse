package databeans.genericIntervalTree

import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpec
import java.util

/**
 * Specification test for depth.Node.
 *
 * @see [[databeans.genericIntervalTree.Node]]
 */
final class NodeSpec extends AnyFunSpec with Matchers {

  val NO_RESULT = new util.ArrayList()

  describe("depth.Node holding a single interval [1, 5]") {
    val intervals = Seq[Interval[Int]](Interval(1, 5, "file1"))
    val node = Node(intervals)

    it("should not be empty") {
      assert(node.isEmpty === false)
    }

    ignore("should has no left or right children") {
      // node.leftNode shouldBe Optional.empty
      // node.rightNode shouldBe Optional.empty
    }

    it("should return 1 result on query interval [4, 8]") {
      node.query(Interval(4, 8, "file2")).size shouldEqual 1
    }

    it("should return 1 result on query interval [2, 3]") {
      node.query(Interval(2, 3, "file2")).size shouldEqual 1
    }

    it("should return 1 result on query interval [0, 1]") {
      node.query(Interval(0, 1, "file2")).size shouldEqual 1
    }

    it("should return 0 result on query interval [-1, 0]") {
      node.query(Interval(-1, 0, "file2")).size shouldEqual 0
    }

    it("should return 1 result on query interval [6, 7]") {
      node.query(Interval(6, 7, "file2")).size shouldEqual 0
    }
  }

  describe("depth.Node holding many intervals") {
    val intervals = Seq[Interval[Int]](
      Interval(1, 5, "file1"),
      Interval(6, 9, "file2"),
      Interval(10, 14, "file3")
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    ignore("should has 1 left and 1 right child") {
      // node.leftNode.get.size shouldEqual 1
      // node.rightNode.get.size shouldEqual 1
    }

    it("should return 2 results on query interval [6, 19]") {
      node.query(Interval(6, 19, "file4")).size shouldEqual 2
    }
  }

  describe("depth.Node holding same interval multiple times") {
    val intervals = Seq[Interval[Int]](
      Interval(1, 355, "file1"),
      Interval(1, 355, "file2"),
      Interval(1, 355, "file3")
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    ignore("should has 1 left and 1 right child") {
      // node.leftNode.get.size shouldEqual 1
      // node.rightNode.get.size shouldEqual 1
    }

    it("should return 3 results on query interval [6, 19]") {
      node.query(Interval(6, 19, "file4")).size shouldEqual 3
    }

    it("should return 3 results on query interval [300, 400]") {
      node.query(Interval(300, 400, "file4")).size shouldEqual 3
    }

    it("should return 0 results on query interval [360, 400]") {
      node.query(Interval(360, 400, "file4")).size shouldEqual 0
    }
  }

  describe("depth.Node holding sorted intervals") {
    val intervals = Seq[Interval[Int]](
      Interval(1, 1139, "file1"),
      Interval(1139, 2368, "file2"),
      Interval(2368, 3503, "file3"),
      Interval(3503, 4745, "file4"),
      Interval(4745, 5999, "file5"),
      Interval(5999, 7200, "file6")
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    ignore("should has 1 left and 1 right child") {
      // node.leftNode.get.size shouldEqual 1
      // node.rightNode.get.size shouldEqual 1
    }

    it("should return 2 results on the frist and last files and 3 else") {
      node.query(Interval(1, 1139, "file1")).size shouldEqual 2
      node.query(Interval(1139, 2368, "file2")).size shouldEqual 3
      node.query(Interval(2368, 3503, "file3")).size shouldEqual 3
      node.query(Interval(3503, 4745, "file4")).size shouldEqual 3
      node.query(Interval(4745, 5999, "file5")).size shouldEqual 3
      node.query(Interval(5999, 7200, "file6")).size shouldEqual 2
    }
  }
}
