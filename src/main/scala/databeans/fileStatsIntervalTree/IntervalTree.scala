package databeans.fileStatsIntervalTree

case class IntervalTree(head: Node, intervals: Seq[Interval]) {

  /**
   * @return the number of intervals in the tree.
   */
  def size: Int = intervals.size

  /**
   * @return true if the tree is empty, false otherwise
   */
  def isEmpty: Boolean = intervals.isEmpty

  /**
   * @return true if the tree is not empty, false otherwise
   */
  def nonEmpty: Boolean = intervals.nonEmpty

  /**
   * Runs an range query.
   *
   * @note Builds the tree in case it is out of sync.
   * @param start the start of the interval
   * @param end   the end of the interval
   * @return a sequence of associated data for all intervals
   *         containing the target point
   */
  def get(i: Interval): List[String] = getIntervals(i).map(_.fileName)

  /**
   * Runs an range query.
   *
   * @note Builds the tree in case it is out of sync.
   * @param start the start of the interval
   * @param end   the end of the interval
   * @return all intervals containing the target point
   */
  def getIntervals(i: Interval, inclusive: Boolean = true): List[Interval] =
    head.query(i, inclusive)
}

object IntervalTree {
  /**
   * An depth.IntervalTree holding the given intervals.
   *
   * @param intervals a sequence of intervals
   * @tparam T the type of data being stored
   * @return an depth.IntervalTree instance
   */
  def apply(intervals: Seq[Interval]): IntervalTree =
    IntervalTree(Node(intervals), intervals)
}
