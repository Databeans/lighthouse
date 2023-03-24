package databeans.fileStatsIntervalTree

import org.apache.spark.sql.types.{DataType, DecimalType, IntegerType, LongType}

case class IntervalBoundary(value: String, statsType: DataType) extends Comparable[IntervalBoundary] {

  def greaterThenOrEqual(a: String, b: String): Boolean = {
    if (compare(a, b) == -1) false else true
  }

  def greaterThenOrEqual(b: String): Boolean = {
    if (compare(value, b) == -1) false else true
  }

  def greaterThen(b: String): Boolean = {
    if (compare(value, b) == 1) true else false
  }

  override def compareTo(o: IntervalBoundary): Int = {
    compare(value, o.value)
  }

  def compare(a: String, b: String): Int = {
    statsType match {
      case IntegerType => compare[Int](a.toInt, b.toInt)
      case LongType => compare[Long](a.toLong, b.toLong)
      case DecimalType() =>
        compare[BigDecimal](
          new BigDecimal(new java.math.BigDecimal(a)),
          new BigDecimal(new java.math.BigDecimal(b)))
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

  def min(a: String, b: String): String = {
    val comp = compare(a, b)
    if (comp == 1) b else a
  }
}