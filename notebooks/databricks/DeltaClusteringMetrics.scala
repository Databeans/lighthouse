// Databricks notebook source
import com.databricks.sql.transaction.tahoe.DeltaLog
import fr.databeans.lighthouse.metrics.delta.DeltaClusteringMetricsBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
case class DeltaClusteringMetrics(deltaLog: DeltaLog, spark: SparkSession) extends DeltaClusteringMetricsBase(spark) {

  override def schema: StructType = deltaLog.snapshot.schema

  override def statsSchema: StructType = deltaLog.snapshot.statsSchema

  override def stateWithStats: DataFrame = deltaLog.snapshot.stateDF

  override def allColumns: Seq[String] = deltaLog.snapshot.schema.map(_.name)

  override def partitionColumns: Seq[String] = deltaLog.snapshot.metadata.partitionColumns
}

object DeltaClusteringMetrics {

  def forName(deltaTable: String, spark: SparkSession): DeltaClusteringMetrics = {
    val location = spark.sql(s"describe detail $deltaTable").select("location").collect()(0)(0).toString
    val deltaLog = DeltaLog.forTable(spark, location)
    DeltaClusteringMetrics(deltaLog, spark)
  }

  def forPath(deltaPath: String, spark: SparkSession): DeltaClusteringMetrics = {
    val deltaLog = DeltaLog.forTable(spark, deltaPath)
    DeltaClusteringMetrics(deltaLog, spark)
  }
}










