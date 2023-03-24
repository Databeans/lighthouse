package databeans.metrics

import databeans.fileStatsIntervalTree.Interval
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DatabricksDeltaClusteringMetrics {

  val STATS_COLUMN = "stats"
  val STATS_MIN_PREFIX = "stats.minValues"
  val STATS_MAX_PREFIX = "stats.maxValues"
  val FILE_RELATIVE_PATH = "add.path"

  def compute(stateWithStats: DataFrame, schema: StructType, statsSchema: StructType, column: String): ClusteringMetrics = {
    val intervals = prepareIntervals(stateWithStats, schema, statsSchema, column)
    ClusteringMetrics.computeMetrics(intervals)
  }

  // TODO get column type from the stats instead of schema for more simplicity.
  def prepareIntervals(stateWithStats: DataFrame, schema: StructType, statsSchema: StructType, column: String): Seq[Interval] = {
    val dataType = getStatsType(schema, column)
    stateWithStats
      .filter(col("add").isNotNull)
      .withColumn(STATS_COLUMN, from_json(col(s"add.$STATS_COLUMN"), statsSchema))
      .select(
        col(s"$FILE_RELATIVE_PATH"),
        col(s"$STATS_MIN_PREFIX.$column").cast(StringType).as("min"),
        col(s"$STATS_MAX_PREFIX.$column").cast(StringType).as("max")
      )
      .filter(col("min").isNotNull and col("max").isNotNull)
      .collect()
      .map { row =>
        Interval(row.getString(1), row.getString(2), row.getString(0), dataType)
      }
  }

  def getStatsType(schema: StructType, column: String) = {
    schema
      .filter(_.name == column)
      .head
      .dataType
  }
}
