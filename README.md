# DeltaClusteringMetrics

## OVERVIEW
___ 
DeltaClusteringMetrics is an open-source library developed by DataBeans to optimize Delta lake performance and cost-effectiveness.  
It is designed to monitor the health of Delta tables from a data layout perspective and provides valuable insights on how data is distributed inside parquet files that make up the Delta table.  
This information helps users to identify when data maintenance operations (vacuum, optimize, Z-order …) should be performed, which improve query performance and reduce storage costs.  
Admittedly, this library is an essential step to achieving the best performance for data lakes while keeping costs at a minimum.  
## SETUP INSTRUCTIONS
___
### Prerequisites
- Apache Spark 3.2.0 installed and running  
- DeltaClusteringMetrics JAR file  
- Delta 2.0.0  
- A Delta table to analyze  
### Using Spark Shell  
1. Open terminal  
2. Run the following command:  ``` spark-shell \
--packages io.delta:delta-core_2.12:2.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
--jars /path/to/clusteringinfo_2.12-0.1.1.jar ```  
**PS:**   Replace /path/to/clusteringinfo_2.12-0.1.1.jar with the actual path to the clusteringinfo_2.12-0.1.1 jar file  
3. Import the DeltaClusteringMetrics class :  
```import databeans.metrics.delta.DeltaClusteringMetrics```  
4. Compute clustering metrics for a specific column in your Delta table:  
```val clusteringMetrics = DeltaClusteringMetrics.forPath("path/to/your/deltaTable", spark).computeForColumn("col")```  
**PS:**   Replace path/to/your/deltaTable with the actual path to your Delta table and col with the name of the column you want to compute clustering metrics for.  
5. Display the computed clustering metrics using the show() method:  
```clusteringMetrics.show() ```
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
This command specifies the following options:  
- --class: Name of the main class of your application.  
- --master: URL of the Spark cluster to use.  
- --packages: Maven coordinates of the Delta Lake library to use.  
- --jars: Path to the clusteringinfo_2.12-0.1.1.jar file.  
- The path to your application's JAR file.  
**PS:**   Make sure to replace <master-url> with the URL of your Spark cluster, and replace the paths to the JAR files with the actual paths on your machine.    
Example:
```  
spark-submit
--class Quickstart 
--master local[*] 
--packages io.delta:delta-core_2.12:2.0.0 
--jars lib/clusteringinfo_2.12-0.1.1.jar 
target/scala-2.12/clustering-metrics-example_2.12-0.1.jar
```  
### Using DATABRICKS  
1. Download the notebook and import it to your workspace. You can download it from [here](https://bitbucket.org/data_beans/clustering-info/src/user-guide/databricksNotebook/DeltaClusteringMetrics.dbc).  
2. Add the clusteringinfo_2.12-0.1.1.jar to your cluster.  
3. Create a new cell.  
4. Insert ```%run path/to/DeltaClusteringMetrics```.  
   **PS:**   Replace path/to/your/DeltaClusteringMetrics with the actual path to your DeltaClusteringMetrics notebook.  
5. Run the cell.   
With these steps completed, you should be able to use the DeltaClusteringMetrics library.  
## CLUSTERING METRICS
___ 
let’s take a look at the next delta table  
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
- forName(“ tableName ”): Name of the Delta Table.  
***
- forPath(“ Path ”): Path for the Delta Table.  
***
- computeForColumn(“columnName”): extract clustering information for a certain column.  
example:  
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  .computeForColumn("keys")
```
***
- computeForColumns(“col1”,”col2”,…): extract clustering information for multiple columns.  
  example:
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  .computeForColumns("keys","values")
```
***
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
- DeltaClusteringMetrics cannot compute metrics for:  
     * A column without statistics: Delta tables require statistics to be computed on the columns before they can be used for clustering, so if statistics are not available, DeltaClusteringMetrics will not be able to compute metrics for that column.  
     * A non-existent column: The column used for clustering must exist in the Delta table, otherwise, DeltaClusteringMetrics will not be able to compute metrics for it.  
     * Partitioning columns: The columns used for partitioning a Delta table cannot be used for clustering, so DeltaClusteringMetrics will not compute metrics for them.  
- DeltaClusteringMetrics is only compatible with Delta tables and may not work with other table formats such as Parquet or ORC.  
- When handling a column with all null values, ```the average_overlap``` and ```average_overlap_depth``` metrics will be assigned a value of -1, while the ```file_depth_histogram``` metric will be assigned a null value.  

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

