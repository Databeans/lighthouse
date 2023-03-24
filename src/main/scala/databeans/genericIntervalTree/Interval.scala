package databeans.genericIntervalTree

case class Interval[T: Ordering](start: T, end: T, fileName: String) extends Comparable[Interval[T]] {

  /**
   * Checks if the interval intersects (or touches) another interval.
   *
   * @param i an interval
   * @return true if the interval intersects the given interval.
   */
  def intersects(min: T, max: T): Boolean = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps

    start <= max && end >= min
  }

  def compare(a: T, b: T): Int = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps

    if (a < b) -1
    else if (a > b) 1
    else 0
  }

  def contains(point: T): Boolean = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps

    point > start && point < end
  }

  override def compareTo(o: Interval[T]): Int = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps

    if (start < o.start) -1
    else if (start > o.start) 1
    else compare(end, o.end)
  }
}
