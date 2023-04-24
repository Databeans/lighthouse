package fr.databeans.fileStatsIntervalTree

import org.apache.spark.sql.types.IntegerType
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

final class NodeSpec extends AnyFunSpec with Matchers {

  describe("depth.Node holding a single interval [1, 5]") {
    val intervals = Seq[Interval](Interval("1", "5", "file1", IntegerType))
    val node = Node(intervals)

    it("should not be empty") {
      assert(node.isEmpty === false)
    }

    it("should has no left or right children") {
      node.left shouldBe None
      node.right shouldBe None
    }

    it("should return 1 result on query interval [4, 8]") {
      node.query(Interval("4", "8", "file2", IntegerType)).size shouldEqual 1
    }

    it("should return 1 result on query interval [2, 3]") {
      node.query(Interval("2", "3", "file2", IntegerType)).size shouldEqual 1
    }

    it("should return 1 result on query interval [0, 1]") {
      node.query(Interval("0", "1", "file2", IntegerType)).size shouldEqual 1
    }

    it("should return 0 result on query interval [-1, 0]") {
      node.query(Interval("-1", "0", "file2", IntegerType)).size shouldEqual 0
    }

    it("should return 1 result on query interval [6, 7]") {
      node.query(Interval("6", "7", "file2", IntegerType)).size shouldEqual 0
    }
  }

  describe("Node holding many intervals") {
    val intervals = Seq(
      Interval("1", "5", "file1", IntegerType),
      Interval("6", "9", "file2", IntegerType),
      Interval("10", "14", "file3", IntegerType)
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    it("should has 1 left and 1 right child") {
      node.left.get.size shouldEqual 1
      node.right.get.size shouldEqual 1
    }

    it("should return 2 results on query interval [6, 19]") {
      node.query(Interval("6", "19", "file4", IntegerType)).size shouldEqual 2
    }
  }

  describe("Node holding same interval multiple times") {
    val intervals = Seq[Interval](
      Interval("1", "355", "file1", IntegerType),
      Interval("1", "355", "file2", IntegerType),
      Interval("1", "355", "file3", IntegerType)
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    it("should has no left or right children") {
      node.left shouldBe None
      node.right shouldBe None
    }

    it("should return 3 results on query interval [6, 19]") {
      node.query(Interval("6", "19", "file4", IntegerType)).size shouldEqual 3
    }

    it("should return 3 results on query interval [300, 400]") {
      node.query(Interval("300", "400", "file4", IntegerType)).size shouldEqual 3
    }

    it("should return 0 results on query interval [360, 400]") {
      node.query(Interval("360", "400", "file4", IntegerType)).size shouldEqual 0
    }
  }

  describe("Node holding sorted intervals") {
    val intervals = Seq[Interval](
      Interval("1", "1139", "file1", IntegerType),
      Interval("1139", "2368", "file2", IntegerType),
      Interval("2368", "3503", "file3", IntegerType),
      Interval("3503", "4745", "file4", IntegerType),
      Interval("4745", "5999", "file5", IntegerType),
      Interval("5999", "7200", "file6", IntegerType)
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    it("should has 1 left and 1 right child") {
      println(node)
      node.left.get.size shouldEqual 2
      node.right.get.size shouldEqual 2
    }

    it("should return 2 results on the first and last files and 3 else") {
      node.query(Interval("1", "1139", "file1", IntegerType)).size shouldEqual 2
      node.query(Interval("1139", "2368", "file2", IntegerType)).size shouldEqual 3
      node.query(Interval("2368", "3503", "file3", IntegerType)).size shouldEqual 3
      node.query(Interval("3503", "4745", "file4", IntegerType)).size shouldEqual 3
      node.query(Interval("4745", "5999", "file5", IntegerType)).size shouldEqual 3
      node.query(Interval("5999", "7200", "file6", IntegerType)).size shouldEqual 2
    }
  }

  describe("Bug1 Node holding many intervals") {
    val intervals = Seq[Interval](
      Interval("1", "5", "file2", IntegerType),
      Interval("0", "7", "file3", IntegerType),
      Interval("11", "16", "file3", IntegerType),
      Interval("7", "16", "file5", IntegerType),
      Interval("5", "9", "file6", IntegerType),
      Interval("4", "16", "file6", IntegerType),
      Interval("0", "13", "file6", IntegerType),
      Interval("9", "12", "file6", IntegerType),
      Interval("7", "9", "file6", IntegerType),
      Interval("20", "30", "file6", IntegerType),
      Interval("31", "40", "file6", IntegerType)
    )

    val node = Node(intervals)

    it("should not be empty") {
      node.isEmpty shouldBe false
    }

    it("should return 2 results on the first and last files and 3 else") {
      node.query(Interval("11", "16", "file1", IntegerType)).size shouldEqual 5
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

    it("should return 2 results on the first and last files and 3 else") {
      node.query(Interval("11", "16", "file1", IntegerType)).size shouldEqual 5
    }
  }

}
