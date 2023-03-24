package databeans.metrics

import databeans.fileStatsIntervalTree.Interval
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.{DeltaLog => OssDeltaLog}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}


object DeltaClusteringMetrics {

  val STATS_COLUMN = "stats"
  val STATS_MIN_PREFIX = "stats.minValues"
  val STATS_MAX_PREFIX = "stats.maxValues"
  val FILE_RELATIVE_PATH = "add.path"

  def compute(spark: SparkSession, deltaPath: String, column: String): ClusteringMetrics = {
    val intervals = prepareIntervals(spark, deltaPath, column)
    ClusteringMetrics.computeMetrics(intervals)
  }

  // TODO check for partition column
  // TODO add support for time travel
  def prepareIntervals(spark: SparkSession, deltaPath: String, column: String): Seq[Interval] = {

    val deltaLog = OssDeltaLog.forTable(spark, deltaPath)

    deltaLog.snapshot.metadata.id
    //TODO: this is too severe. maybe just throw a warning instead.
    assert(!isPartitioningColumn(deltaLog, column),
      s"'$column' is a partitioning column. Clustering metrics cannot be computed for partitioning column")

    val dataType = getStatsType(deltaLog, column)
    val statsSchema = deltaLog.snapshot.statsSchema

    //TODO: this is to severe. maybe just throw a warning instead.
    assert(checkIfColumnExists(column, statsSchema), s"no statistics found for column '$column'")

    deltaLog
      .snapshot.stateDF
      .filter(col("add").isNotNull)
      .withColumn(STATS_COLUMN, from_json(col(s"add.$STATS_COLUMN"), statsSchema))
      .select(
        col(s"$FILE_RELATIVE_PATH"),
        col(s"$STATS_MIN_PREFIX.$column").cast(StringType).as("min"),
        col(s"$STATS_MAX_PREFIX.$column").cast(StringType).as("max")
      ).collect()
      .map { row =>
        Interval(row.getString(1), row.getString(2), row.getString(0), dataType)
      }
  }

  def getStatsType(deltaLog: OssDeltaLog, column: String) = {
    val extractedColumn = deltaLog
      .snapshot
      .schema
      .filter(_.name == column)

    assert(extractedColumn.nonEmpty, "column not found")
    extractedColumn.head.dataType
  }

  def isPartitioningColumn(deltaLog: OssDeltaLog, column: String): Boolean = {
    deltaLog.snapshot.metadata.partitionColumns.contains(column)
  }

  def checkIfColumnExists(column: String, statsSchema: StructType): Boolean = {
    statsSchema.fields.filter(_.name == "minValues")
      .map(_.dataType)
      .flatMap {
        case StructType(f) => f
      }.map(_.name)
      .contains(column)
  }

  def getSizePerFile(deltaLog: OssDeltaLog) = {
    deltaLog.snapshot.allFiles.collect()
      .map(addFile => addFile.size.toDouble).toList
  }
}
