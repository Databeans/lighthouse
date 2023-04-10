package fr.databeans.fileStatsIntervalTree

import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType}

import scala.collection.SortedMap
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer

case class Node private(
                         center: String,
                         left: Option[Node],
                         right: Option[Node],
                         intervals: SortedMap[Interval, List[Interval]]) {


  def size: Int = intervals.size

  def isEmpty: Boolean = intervals.isEmpty

  def query(i: Interval, inclusive: Boolean = true): List[Interval] = {

    val result = ListBuffer.empty[Interval]

    intervals.takeWhile {
      case (key, list) =>
        val overlap = if (inclusive) {
          key.intersects(i.start, i.end)
        }
        else {
          key.exclusiveIntersects(i.start, i.end)
        }
        if (overlap) list.foreach(result += _)
        if (i.compare(key.start, i.end) == 1) false else true
    }

    if (!i.greaterThenPoint(center) && left.isDefined)
      result ++= left.get.query(i, inclusive)

    if (!i.lowerThenPoint(center) && right.isDefined)
      result ++= right.get.query(i, inclusive)

    result.toList
  }
}

object Node {

  private def medianOf(set: TreeSet[_ >: Int with Long with BigDecimal with String]): Option[String] = {
    val mid = set.size / 2

    set.zipWithIndex.find(_._2 == mid) match {
      case None => None
      case Some((point, _)) => Some(point.toString)
    }
  }

  def getMedian(intervals: Seq[Interval]): Option[String] = {
    val statsType = intervals.map(_.statsType).head
    statsType match {
      case IntegerType => {
        var endpoints = TreeSet.empty[Int]
        intervals.foreach { interval =>
          endpoints += interval.start.toInt
          endpoints += interval.end.toInt
        }
        medianOf(endpoints)
      }
      case LongType => {
        var endpoints = TreeSet.empty[Long]
        intervals.foreach { interval =>
          endpoints += interval.start.toLong
          endpoints += interval.end.toLong
        }
        medianOf(endpoints)
      }
      case DecimalType() => {
        var endpoints = TreeSet.empty[BigDecimal]
        intervals.foreach { interval =>
          endpoints += new BigDecimal(new java.math.BigDecimal(interval.start))
          endpoints += new BigDecimal(new java.math.BigDecimal(interval.end))
        }
        medianOf(endpoints)
      }

      case _ => {
        var endpoints = TreeSet.empty[String]
        intervals.foreach { interval =>
          endpoints += interval.start
          endpoints += interval.end
        }
        medianOf(endpoints)
      }
    }
  }

  def apply(intervals: Seq[Interval]): Node = {

    var intervalsMap = SortedMap.empty[Interval, List[Interval]]

    val median = getMedian(intervals).get

    var leftNodes = List.empty[Interval]
    var rightNodes = List.empty[Interval]

    intervals.foreach { interval =>
      if (interval.lowerThenPoint(median)) leftNodes ::= interval

      else if (interval.greaterThenPoint(median)) rightNodes ::= interval

      else intervalsMap ++= Seq(interval -> (interval :: intervalsMap.getOrElse(interval, List.empty)))
    }


    if (leftNodes.nonEmpty && rightNodes.nonEmpty) {
      Node(median, Some(Node(leftNodes)), Some(Node(rightNodes)), intervalsMap)
    } else if (leftNodes.nonEmpty)
      Node(median, Some(Node(leftNodes)), None, intervalsMap)
    else if (rightNodes.nonEmpty)
      Node(median, None, Some(Node(rightNodes)), intervalsMap)
    else
      Node(median, None, None, intervalsMap)
  }
}