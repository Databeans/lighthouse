package fr.databeans.lighthouse.fileStatsIntervalTree

import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, LongType}

case class Interval(start: String, end: String, fileName: String, statsType: DataType)
  extends Comparable[Interval] {

  def intersects(min: String, max: String): Boolean = {
    greaterThenOrEqual(max, start) && greaterThenOrEqual(end, min)
  }

  def exclusiveIntersects(min: String, max: String): Boolean = {
    greaterThen(max, start) && greaterThen(end, min)
  }

  def greaterThenOrEqual(a: String, b: String): Boolean = {
    if (compare(a, b) == -1) false else true
  }

  def greaterThen(a: String, b: String): Boolean = {
    if (compare(a, b) == 1) true else false
  }

  override def compareTo(o: Interval): Int = {
    val compareStarts = compare(start, o.start)
    if (compareStarts != 0) {
      compareStarts
    }
    else compare(end, o.end)
  }

  def compare(a: String, b: String): Int = {
    statsType match {
      case IntegerType => compare[Int](a.toInt, b.toInt)
      case LongType => compare[Long](a.toLong, b.toLong)
      case DecimalType() => {
        compare[BigDecimal](
          new BigDecimal(new java.math.BigDecimal(a)),
          new BigDecimal(new java.math.BigDecimal(b)))
      }
      case _ => compare[String](a, b)
    }
  }

  def compare[T: Ordering](a: T, b: T): Int = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps
    if (a < b) -1
    else if (a > b) 1
    else 0
  }

  def lowerThenPoint(median: String): Boolean = {
    val comp = compare(end, median)
    if (comp == -1) true else false
  }

  def greaterThenPoint(median: String): Boolean = {
    val comp = compare(median, start)
    if (comp == -1) true else false
  }

  def greaterThenOrEqualPoint(median: String): Boolean = {
    val comp = compare(median, start)
    if (comp == 1) false else true
  }

  def lowerThenPointOrEqual(median: String): Boolean = {
    val comp = compare(end, median)
    if (comp == 1) false else true
  }

  def startsBefore(point: String): Boolean = {
    val comp = compare(start, point)
    if (comp == 1) false else true
  }

  def endsAfter(point: String): Boolean = {
    val comp = compare(end, point)
    if (comp == 1) true else false
  }

  def min(a: String, b: String): String = {
    val comp = compare(a, b)
    if (comp == 1) b else a
  }

  def max(a: String, b: String): String = {
    val comp = compare(a, b)
    if (comp == 1) a else b
  }
}