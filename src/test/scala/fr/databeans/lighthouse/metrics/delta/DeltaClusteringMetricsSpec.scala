package fr.databeans.lighthouse.metrics.delta

import fr.databeans.lighthouse.metrics.Distribution
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaExtendedSparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class DeltaClusteringMetricsSpec extends QueryTest with SharedSparkSession with DeltaExtendedSparkSession {

  def buildHistogram(maxBin: Int, populatedBuckets: Map[Double, Int]): Map[Double, Int] = {
    val missingBins = Distribution.computeUnPopulatedBuckets(maxBin, populatedBuckets)
    missingBins ++ populatedBuckets
  }

  def getStats(deltaPath: String, column: String) = {
    DeltaLog.forTable(spark, deltaPath).snapshot.withStats
      .select(
        col("path"),
        col(s"stats.minValues.$column").as("min"),
        col(s"stats.maxValues.$column").as("max")
      )
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("compute metrics for a delta table with non overlapping files") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .write.mode("overwrite")
        .format("delta")
        .save(dir.toString)


      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
      val metrics = deltaClusteringMetric.computeForColumn("id")
      checkAnswer(metrics, Row("id", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0))
    }
  }

  test("compute metrics for a delta table with all overlapping files") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .withColumn("key", lit(1))
        .write.mode("overwrite")
        .format("delta")
        .save(dir.toString)

      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
      val metrics = deltaClusteringMetric.computeForColumn("key")
      checkAnswer(metrics, Row("key", 5L, 5L, 5.0, buildHistogram(16, Map((5.0, 5))), 4.0))
    }
  }

  test("compute metrics for table defined by name") {
    withTable("deltaTable") {
      spark.range(1, 50, 1, 5).toDF()
        .write.format("delta").saveAsTable("deltaTable")

      val deltaClusteringMetric = DeltaClusteringMetrics.forName("deltaTable", spark)
      val metrics = deltaClusteringMetric.computeForColumn("id")
      checkAnswer(metrics, Row("id", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0))
    }
  }

  test("compute metrics for a column without statistics") {
    withTempDir { dir =>
      val data = spark.range(1, 50, 1, 5).toDF()
        .withColumn("value", col("id") * 3)

      data
        .filter("1 > 2")
        .write.mode("append")
        .format("delta").save(dir.toString)

      spark.sql(s"ALTER TABLE delta.`${dir.toString}` SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')")

      data
        .write.mode("append")
        .format("delta").save(dir.toString)

      val thrown = intercept[AssertionError] {
        val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
        deltaClusteringMetric.computeForColumn("value")
      }
      assert(thrown.getMessage === "assertion failed: no statistics found for column 'value'")
    }
  }

  test("compute metrics for a non existent column") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .write.format("delta").save(dir.toString)

      val thrown = intercept[AssertionError] {
        val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
        deltaClusteringMetric.computeForColumn("non_existent_column")
      }
      assert(thrown.getMessage.contains("assertion failed: column non_existent_column not found in columns"))
    }
  }

  test("compute metrics for all columns of the table") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .withColumn("id", col("id").cast(IntegerType))
        .withColumn("value", lit(1))
        .write.mode("overwrite")
        .format("delta").save(dir.toString)

      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
      val metrics = deltaClusteringMetric.computeForAllColumns()

      checkAnswer(
        metrics,
        Seq(
          Row("id", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0),
          Row("value", 5L, 5L, 5.0, buildHistogram(16, Map((5.0, 5))), 4.0)
        )
      )
    }
  }

  test("compute metrics for a subset columns of the table") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .withColumn("id", col("id"))
        .withColumn("value1", lit(1))
        .withColumn("value2", lit(2))
        .write.format("delta").save(dir.toString)

      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
      val metrics = deltaClusteringMetric.computeForColumns("id", "value1")

      checkAnswer(
        metrics,
        Seq(
          Row("id", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0),
          Row("value1", 5L, 5L, 5.0, buildHistogram(16, Map((5.0, 5))), 4.0)
        )
      )
    }
  }

  test("compute metrics for supported Data Types") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .withColumn("value_int", col("id").cast(IntegerType))
        .withColumn("value_long", col("id").cast(LongType))
        .withColumn("value_decimal", col("id").cast(DecimalType(4, 2)))
        .withColumn("value_string", format_string("%02d", col("id")))
        .drop("id")
        .write.format("delta").save(dir.toString)

      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)
      val metrics = deltaClusteringMetric.computeForAllColumns()

      checkAnswer(
        metrics,
        Seq(
          Row("value_int", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0),
          Row("value_long", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0),
          Row("value_decimal", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0),
          Row("value_string", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0)
        )
      )
    }
  }

  test("compute metrics for a partitioned delta table") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .withColumn("part", col("id") % 3)
        .write.partitionBy("part").format("delta").save(dir.toString)

      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)

      val errorMessage = "assertion failed: 'part' is a partitioning column. Clustering metrics cannot be computed for partitioning columns"

      // computeForColumn should fail
      val thrown1 = intercept[AssertionError] {
        deltaClusteringMetric.computeForColumn("part")
      }
      assert(thrown1.getMessage == errorMessage)

      // computeForColumns should fail
      val thrown2 = intercept[AssertionError] {
        deltaClusteringMetric.computeForColumns("part", "id")
      }
      assert(thrown2.getMessage == errorMessage)

      // computeForAllColumns should compute metrics for non partitioning columns only.
      val metrics = deltaClusteringMetric.computeForAllColumns()
      checkAnswer(metrics, Seq(Row("id", 15L, 0L, 2.3333, buildHistogram(16, Map((3.0, 15))), 2.0)))
    }
  }

  test("compute metrics for column with null values") {
    withTempDir { dir =>
      spark.range(1, 50, 1, 5).toDF()
        .withColumn("value_1", when(col("id") % 10 === 1, null).otherwise(col("id")))
        .withColumn("value_2", when(col("id") < 20, null).otherwise(col("id")))
        .withColumn("value_3", lit(null).cast(StringType))
        .write.format("delta").save(dir.toString)

      val deltaClusteringMetric = DeltaClusteringMetrics.forPath(dir.toString, spark)

      val value1Metrics = deltaClusteringMetric.computeForColumn("value_1")
      checkAnswer(value1Metrics, Seq(Row("value_1", 5L, 0L, 1.0, buildHistogram(16, Map((1.0, 5))), 0.0)))

      // null intervals included in total_file_count and total_uniform_file_count but excluded from other metrics.
      val value2Metrics = deltaClusteringMetric.computeForColumn("value_2")
      checkAnswer(value2Metrics, Seq(Row("value_2", 5L, 2L, 1.0, buildHistogram(16, Map((1.0, 3))), 0.0)))

      // all intervals are null
      val value3Metrics = deltaClusteringMetric.computeForColumn("value_3")
      checkAnswer(value3Metrics, Seq(Row("value_3", 5L, 5L, -1, null.asInstanceOf[Map[Double, Int]], -1)))
    }
  }
}

