# DeltaClusteringMetrics

## OVERVIEW
___ 
DeltaClusteringMetrics is an open-source library developed by Databeans in 2023 that enables Delta lake users to optimize their data for better performance and cost-effectiveness.  
By analyzing the data layout of Delta tables, the library extracts valuable insights into how the data is distributed within the tables and provides metrics that can help data engineers and analysts make informed decisions about data maintenance operations. These operations, such as vacuuming or compacting, can improve query performance and reduce storage costs.  
Overall, DeltaClusteringMetrics enhances the user experience by allowing them to assess query performance without running them and identify when data maintenance operations should be performed on the table.  
With its ability to provide a detailed overview of Delta tables, this library is an essential tool for anyone looking to improve the performance and cost-effectiveness of their data lake.  
## SETUP INSTRUCTIONS
___
### Prerequisites
- Apache Spark 3.x installed and running  
- DeltaClusteringMetrics JAR file  
- A Delta table to analyze  
### Using Spark Shell

### Using spark-submit
Submit the application to the Spark cluster:
``` 
spark-submit \
   --class com.example.MyApp \
   --master <master-url> \
   --packages io.delta:delta-core_2.12:2.0.0 \
   --jars /path/to/clusteringinfo_2.12-0.1.1.jar \
   --/path/to/your/spark/application.jar
```
## CLUSTERING METRICS
___ 
let’s suppose you have this delta table  
```
spark.range(1, 5, 1).toDF()
.withColumn("id", col("id").cast(IntegerType))
.withColumn("keys", lit(1))
.withColumn("values", col("id") * 3)
.write.mode("overwrite")
.format("delta")
.save("examples/target/DeltaClusteringMetrics")
```  
Using DeltaClusteringMetrics:  
```
import databeans.metrics.delta.DeltaClusteringMetrics
```
```
val clusteringMetric = DeltaClusteringMetrics
 .forPath("examples/target/DeltaClusteringMetrics", spark)
 .computeForColumn("keys")
```
The library will then compute the clustering metrics and generate a dataframe containing the next columns:  

DeltaClusteringMetrics detects several metrics including:  

| column | total_file_count | total_uniform_file_count | average_overlap | average_overlap_depth | file_depth_histogram |
|--------|------------------|--------------------------|-----------------|-----------------------|----------------------|
| Keys   | 5                | 5                        | 3.0             | 4 .0                  | {5.0 -> 0, 10.0 -... |  
  

### - total_file_count:  
Total number of files composing the Delta table.  
### - total_uniform_file_count:  
Files in which min and max values of a given ordering column are equal  
### - average_overlap:  
Average number of overlapping files for each file in the delta table.  
this is a simple example of a table consisting of 4 files:  

![](https://miro.medium.com/v2/resize:fit:828/0*_Pi7feo5ZxAdvW8k)  

### - average_overlap_depth:  
The average number of files that will be read when an overlap occurs.    
Throughout this figure, we will study the evolution of the average_overlap_depth of a table containing 4 files:  

![](https://miro.medium.com/v2/resize:fit:1400/0*rmoB3fxNL2kSqijR)  

=> The higher the average_overlap and the average_overlap_depth, the worse the clustering
### - File_depth_histogram:
A histogram detailing the distribution of the overlap_depth on the table by grouping the tables’ files by their proportional overlap depth.  
The histogram contains buckets with widths:
* 0 to 16 with increments of 1.  
* For buckets larger than 16, increments of twice the width of the previous bucket (e.g. 32, 64, 128, …)  
### Parameters
___ 
- forName(“ tableName ”): Name of the Delta Table.  
- forPath(“ Path ”): Path for the Delta Table.  
- computeForColumn(“columnName”): extract clustering information for a certain column.  
example:  
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  .computeForColumn("keys")
```
- computeForColumns(“col1”,”col2”,…): extract clustering information for multiple columns.  
  example:
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  .computeForColumns("keys","values")
```
- computeForAllColumns(): extract clustering information for the entire table.  
  example:  
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  .computeForAllColumns()
```
## LIMITATIONS
___ 
- Supported data types: DeltaClusteringMetrics currently supports the following data types: Int, Long, Decimal, and String.  
- DeltaClusteringMetrics cannot compute metrics for a column without statistics: Delta tables require statistics to be computed on the columns before they can be used for clustering, so if statistics are not available, DeltaClusteringMetrics will not be able to compute metrics for that column.  
- DeltaClusteringMetrics cannot compute metrics for a non-existent column: The column used for clustering must exist in the Delta table, otherwise, DeltaClusteringMetrics will not be able to compute metrics for it.  
- Clustering metrics cannot be computed for partitioning columns: The columns used for partitioning a Delta table cannot be used for clustering, so DeltaClusteringMetrics will not compute metrics for them.  
- DeltaClusteringMetrics is only compatible with Delta tables and may not work with other table formats such as Parquet or ORC.  

## TECHNOLOGIES
___ 
DeltaClusteringMetrics is developed using:  
- Scala 2.12.13  
- Spark 3.2.0  
- Delta 2.0.0  

## CONTRIBUTING
___ 
DeltaClusteringMetrics is an open-source project, and we welcome contributors from the community. If you have a new feature or improvement, feel free to submit a pull request.  

## BLOGS
___
- [Z-ordering: take the Guesswork out (part1)](https://databeans-blogs.medium.com/z-ordre-take-the-guesswork-out-bad0133d7895)  
- [Z-ordering: take the Guesswork out (part2)](https://databeans-blogs.medium.com/delta-z-ordering-take-the-guesswork-out-part2-1bdd03121aec)

