package fr.databeans.lighthouse.fileStatsIntervalTree

case class IntervalTree(head: Node, intervals: Seq[Interval]) {

  def size: Int = intervals.size

  def isEmpty: Boolean = intervals.isEmpty

  def nonEmpty: Boolean = intervals.nonEmpty

  def getIntervals(i: Interval, inclusive: Boolean = true): List[Interval] =
    head.query(i, inclusive)
}

object IntervalTree {
  def apply(intervals: Seq[Interval]): IntervalTree =
    IntervalTree(Node(intervals), intervals)
}
