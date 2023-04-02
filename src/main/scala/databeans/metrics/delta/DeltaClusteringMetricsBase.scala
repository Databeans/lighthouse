package databeans.metrics.delta

import databeans.fileStatsIntervalTree.Interval
import databeans.metrics.{ClusteringMetrics, ClusteringMetricsBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DeltaClusteringMetricsBase(spark: SparkSession) extends ClusteringMetricsBuilder {

  val STATS_COLUMN = "stats"
  val STATS_MIN_PREFIX = "stats.minValues"
  val STATS_MAX_PREFIX = "stats.maxValues"
  val FILE_RELATIVE_PATH = "add.path"

  def schema: StructType

  def statsSchema: StructType

  def stateWithStats: DataFrame

  def allColumns: Seq[String]

  def partitionColumns: Seq[String]

  def computeForColumn(column: String): DataFrame = {
    import spark.implicits._

    Seq(column).map(col => compute(col)).toDF()
  }

  def computeForColumns(columns: String*): DataFrame = {
    import spark.implicits._
    columns.map(col => compute(col)).toDF()
  }

  def computeForAllColumns(): DataFrame = {
    import spark.implicits._
    allColumns.diff(partitionColumns).map(col => compute(col)).toDF()
  }

  private def compute(column: String): ClusteringMetrics = {
    val intervals = prepareIntervals(column)
    computeMetrics(column, intervals)
  }

  // TODO add support for time travel
  private def prepareIntervals(column: String): Seq[Interval] = {

    assert(!isPartitioningColumn(column),
      s"'$column' is a partitioning column. Clustering metrics cannot be computed for partitioning columns")

    val dataType = getStatsType(column)

    assert(checkIfStatsExists(column), s"no statistics found for column '$column'")

    stateWithStats
      .filter(col("add").isNotNull)
      .withColumn(STATS_COLUMN, from_json(col(s"add.$STATS_COLUMN"), statsSchema))
      .select(
        col(s"$FILE_RELATIVE_PATH"),
        col(s"$STATS_MIN_PREFIX.$column").cast(StringType).as("min"),
        col(s"$STATS_MAX_PREFIX.$column").cast(StringType).as("max")
      )
      .collect()
      .map { row =>
        Interval(row.getString(1), row.getString(2), row.getString(0), dataType)
      }
  }

  private def getStatsType(column: String): DataType = {
    val extractedColumn = schema
      .filter(_.name == column)

    assert(extractedColumn.nonEmpty, s"column $column not found in columns ${allColumns.mkString(",")}")
    extractedColumn.head.dataType
  }

  // TODO remove hard coded minValues
  private def checkIfStatsExists(column: String): Boolean = {
    statsSchema.fields.filter(_.name == "minValues")
      .map(_.dataType)
      .flatMap {
        case StructType(f) => f
      }.map(_.name)
      .contains(column)
  }

  private def isPartitioningColumn(column: String): Boolean = {
    partitionColumns.contains(column)
  }
}
