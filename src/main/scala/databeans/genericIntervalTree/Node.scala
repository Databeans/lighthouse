package databeans.genericIntervalTree

import scala.collection.SortedMap
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer

class Node[T: Ordering] private(
                                 center: T,
                                 val left: Option[Node[T]],
                                 val right: Option[Node[T]],
                                 intervals: SortedMap[Interval[T], List[Interval[T]]]) {

  /**
   * @return the number of intervals on the node
   */
  def size: Int = intervals.size

  /**
   * @return true if the node contains no data, false otherwise
   */
  def isEmpty: Boolean = intervals.isEmpty

  /**
   * Runs an interval intersection query on the node.
   *
   * @param from range start
   * @param to   range end
   * @return all intervals containing time
   */
  def query(i: Interval[T]): List[Interval[T]] = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps

    val result = ListBuffer.empty[Interval[T]]

    intervals.takeWhile {
      case (key, list) =>
        if (key.intersects(i.start, i.end)) list.foreach(result += _)
        key.start <= i.end
    }

    if (i.start < center && left.isDefined)
      result ++= left.get.query(i)
    if (i.end > center && right.isDefined)
      result ++= right.get.query(i)


    result.toList
  }
}

object Node {

  private def medianOf[T](set: TreeSet[T]): Option[T] = {
    val mid = set.size / 2
    set.zipWithIndex.find(_._2 == mid) match {
      case None => None
      case Some((point, _)) => Some(point)
    }
  }

  /**
   * An node holding the given intervals.
   *
   * @param intervals a sequence of intervals
   * @tparam T the type of data being stored
   * @return an depth.Node instance
   */
  def apply[T: Ordering](intervals: Seq[Interval[T]]): Node[T] = {
    val ord = implicitly[Ordering[T]]
    import ord.mkOrderingOps

    var intervalsMap = SortedMap.empty[Interval[T], List[Interval[T]]]
    var endpoints = TreeSet.empty[T]

    intervals.foreach { interval =>
      endpoints += interval.start
      endpoints += interval.end
    }

    val median = medianOf(endpoints).get

    var leftNodes = List.empty[Interval[T]]
    var rightNodes = List.empty[Interval[T]]

    intervals.foreach { interval =>
      if (interval.end < median) leftNodes ::= interval
      else if (interval.start > median) rightNodes ::= interval
      else intervalsMap ++= Seq(interval -> (interval :: intervalsMap.getOrElse(interval, List.empty)))
    }

    if (leftNodes.nonEmpty && rightNodes.nonEmpty)
      new Node(median, Some(Node(leftNodes)), Some(Node(rightNodes)), intervalsMap)
    else if (leftNodes.nonEmpty)
      new Node(median, Some(Node(leftNodes)), None, intervalsMap)
    else if (rightNodes.nonEmpty)
      new Node(median, None, Some(Node(rightNodes)), intervalsMap)
    else
      new Node(median, None, None, intervalsMap)
  }
}