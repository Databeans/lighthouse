import fr.databeans.lighthouse.metrics.delta.DeltaClusteringMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.IntegerType

object Quickstart {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Quickstart")
      .getOrCreate()
    import spark.implicits._

    spark.range(1, 5, 1).toDF()
      .withColumn("id", col("id").cast(IntegerType))
      .withColumn("keys", lit(1))
      .withColumn("values", col("id") * 3)
      .write.mode("overwrite")
      .format("delta")
      .save("deltaTable")

    val clusteringMetric = DeltaClusteringMetrics
      .forPath("deltaTable", spark)
      .computeForColumn("id")
    clusteringMetric.show()
  }
}
