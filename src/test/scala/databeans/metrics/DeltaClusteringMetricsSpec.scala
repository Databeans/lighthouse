package databeans.metrics

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.functions.{col, expr, from_json, lit, udaf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types._

// TODO Update test to add histogram and overlap average
class DeltaClusteringMetricsSpec extends QueryTest with SharedSparkSession with DeltaExtendedSparkSession {

  def printStats(deltaPath: String, column: String): Unit = {
    DeltaLog.forTable(spark, deltaPath).snapshot.withStats
      .select(
        col("path"),
        col(s"stats.minValues.$column").as("min"),
        col(s"stats.maxValues.$column").as("max")
      ).show()
  }

  test("test overlap for a delta table with non overlapping files") {
    withTempDir { dir =>
      val s = spark
      import s.implicits._
      spark.sparkContext.setLogLevel("ERROR")

      spark.range(1, 50, 1, 5).toDF()
        .withColumn("id", col("id").cast(IntegerType))
        .withColumn("keys", col("id") * 2)
        .withColumn("value", col("id") * 3)
        .write.mode("append")
        .format("delta")
        .save(dir.toString)

      val metrics = DeltaClusteringMetrics.compute(spark, dir.toString, "keys")
      assert(metrics.averageOverlapDepth == 1.0)
    }
  }

  test("test overlap for a delta table with all overlapping files") {
    withTempDir { dir =>
      val s = spark
      import s.implicits._

      spark.sparkContext.setLogLevel("ERROR")

      spark.range(1, 50, 1, 5).toDF()
        .withColumn("id", col("id").cast(IntegerType))
        .withColumn("keys", lit(1))
        .withColumn("value", col("id") * 3)
        .write.mode("append")
        .format("delta")
        .save(dir.toString)

      val metrics = DeltaClusteringMetrics.compute(spark, dir.toString, "keys")
      assert(metrics.averageOverlapDepth == 5)
    }
  }

  ignore("get metrics for a column without statistics") {
    withTempDir { dir =>
      val s = spark
      import s.implicits._

      spark.sparkContext.setLogLevel("ERROR")

      val data = spark.range(1, 50, 1, 5).toDF()
        .withColumn("id", col("id").cast(IntegerType))
        .withColumn("keys", lit(1))
        .withColumn("value", col("id") * 3)

      data
        .filter("1 > 2")
        .write.mode("append")
        .format("delta")
        .save(dir.toString)

      // spark.sql("DROP TABLE IF EXISTS my_table")
      // spark.sql("CREATE TABLE my_table(id int, keys int, value int) USING DELTA")
      spark.sql(s"ALTER TABLE delta.`${dir.toString}` SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')")

      data
        .write.mode("append")
        .format("delta")
        .save(dir.toString)

      spark.read.format("delta").load(dir.toString).show()

      val metrics = DeltaClusteringMetrics.compute(spark, dir.toString, "value")

    }
  }
}

