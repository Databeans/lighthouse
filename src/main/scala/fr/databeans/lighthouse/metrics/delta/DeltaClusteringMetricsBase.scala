package fr.databeans.lighthouse.metrics.delta

import fr.databeans.lighthouse.fileStatsIntervalTree.Interval
import fr.databeans.lighthouse.metrics.{ClusteringMetrics, ClusteringMetricsBuilder}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class DeltaClusteringMetricsBase(spark: SparkSession) extends ClusteringMetricsBuilder {

  val STATS_COLUMN = "stats"
  val MIN_PREFIX = "minValues"
  val MAX_PREFIX = "maxValues"
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
    if (checkIfStatsExistForAllColumnsOfTable()){
      allColumns.diff(partitionColumns).map(col => compute(col)).toDF()
    } else{
      val colsWithoutStats = getNamesOfColumnsWithoutStats
      allColumns.diff(partitionColumns).diff(colsWithoutStats).map(col => compute(col)).toDF()
    }
  }

  private def compute(column: String): ClusteringMetrics = {
    val intervals = prepareIntervals(column)
    computeMetrics(column, intervals)
  }

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
        col(s"${STATS_COLUMN}.${MIN_PREFIX}.$column").cast(StringType).as("min"),
        col(s"${STATS_COLUMN}.${MAX_PREFIX}.$column").cast(StringType).as("max")
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

  private def checkIfStatsExists(column: String): Boolean = {
    statsSchema.fields.filter(_.name == MIN_PREFIX)
      .map(_.dataType)
      .flatMap {
        case StructType(f) => f
      }.map(_.name)
      .contains(column)
  }

  private def checkIfStatsExistForAllColumnsOfTable(): Boolean ={
    val statsExistenceInColumns = allColumns.map(col => checkIfStatsExists(col))
    if (statsExistenceInColumns.contains(false)){
      false
    } else {
      true
    }
  }

  private def getNamesOfColumnsWithoutStats: Seq[String] ={
    var columnsWithoutStats: Seq[String] = Seq.empty[String]

    allColumns.foreach(col =>
      if (!checkIfStatsExists(col)){
        columnsWithoutStats = columnsWithoutStats :+ col
      }
    )
    columnsWithoutStats
  }

  private def isPartitioningColumn(column: String): Boolean = {
    partitionColumns.contains(column)
  }
}
