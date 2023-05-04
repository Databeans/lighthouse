# Lighthouse

## OVERVIEW
 
Lighthouse is a library developed by DataBeans to optimize Lakehouse performance and reduce its total cost ownership.  
It is designed to monitor the health of the Lakehouse tables from a data layout perspective and provide valuable insights about how well the data is clustered.  
This information helps users to identify when data maintenance operations (vacuum, compaction, clustering …) should be performed, which improve query performance and reduce storage costs.  
Lighthouse supports Delta Lake, we plan to expand its capabilities to include other open lakehouse formats in the future.  

## BUILDING

Lighthouse is compiled using SBT.

To compile, run
``` 
sbt compile
``` 

To generate artifacts, run
``` 
sbt package
``` 

To execute tests, run
``` 
sbt test
``` 

## SETUP INSTRUCTIONS

### Prerequisites
- Apache Spark 3.3.2
- Delta 2.3.0
- Lighthouse JAR file
### Using Spark Shell  
1. Open the terminal and run the following command: 
``` 
spark-shell --packages io.delta:delta-core_2.12:2.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" 
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
--jars </path/to/Lighthouse_2.12-0.1.0.jar> 
```  
**PS:**   Replace </path/to/lighthouse_2.12-0.1.0.jar> with the actual path to the Lighthouse_2.12-0.1.0 jar file  
2. Import the DeltaClusteringMetrics class :
```
import fr.databeans.lighthouse.metrics.delta.DeltaClusteringMetrics
```  

3. Compute clustering metrics for a given column of the Delta table:  
```
val clusteringMetrics = DeltaClusteringMetrics.forPath("path/to/your/deltaTable", spark).computeForColumn("col_name")
```  

4. Display the computed clustering metrics using the show() method:  
```
clusteringMetrics.show() 
```

### Using spark-submit
Submit the application to a Spark cluster:
``` 
spark-submit \
   --class com.example.MyApp \
   --master <master-url> \
   --packages io.delta:delta-core_2.12:2.0.0 \
   --jars /path/to/Lighthouse_2.12-0.1.0.jar \
   </path/to/your/spark/application.jar>
```
This command specifies the following options:  
- --class: Name of the main class of your application.  
- --master: URL of the Spark cluster to use.  
- --packages: Maven coordinates of the Delta Lake library to use.  
- --jars: Path to the Lighthouse_2.12-0.1.0.jar file.  
- The path to your application's JAR file.  

Example:
```  
spark-submit
--class Quickstart 
--master local[*] 
--packages io.delta:delta-core_2.12:2.0.0 
--jars lib/Lighthouse_2.12-0.1.0.jar 
target/scala-2.12/clustering-metrics-example_2.12-0.1.jar
```  
### Using DATABRICKS  
1. Add the Lighthouse_2.12-0.1.0.jar to your cluster.
2. Download this [notebook](https://github.com/Databeans/lighthouse/blob/main/notebooks/databricks/DeltaClusteringMetrics.scala) and import it to your workspace.
3. Create a new cell in your notebook and insert ```%run <path/to/DeltaClusteringMetrics>```.

   **PS:**   Replace <path/to/your/DeltaClusteringMetrics> with the actual path to the DeltaClusteringMetrics notebook.  
4. Run the cell.   
With these steps completed, you should be able to use the DeltaClusteringMetrics library.  

## CLUSTERING METRICS

### Syntax

- forName(deltaTable: String, spark: SparkSession): DeltaClusteringMetrics  
     * deltaTable: Name of the Delta table  
     * spark: SparkSession instance
  

- forPath(deltaPath: String, spark: SparkSession): DeltaClusteringMetrics  
     * deltaPath: Path of the Delta table  
     * spark: SparkSession instance  


- computeForColumn(column: String): DataFrame
     * column: column name to compute metrics for
  

- computeForColumns(columns: String*): DataFrame
     * columns: columns list to compute metrics for
  

- computeForAllColumns(): DataFrame


### Usage: 
Assuming that you have a delta table

import DeltaClusteringMetrics
```
import fr.databeans.lighthouse.metrics.delta.DeltaClusteringMetrics
```

compute clustering information for a given column.

```
val clusteringMetric = DeltaClusteringMetrics
 .forPath("path/to/deltaTable", spark)
 .computeForColumn("id")
```

compute clustering information for multiple columns.  

```
val clusteringMetrics = DeltaClusteringMetrics  
  .forName("DeltaTable",spark)  
  .computeForColumns("id","value")  
```

compute clustering information for all columns of the table.  

```
val clusteringMetrics = DeltaClusteringMetrics  
  .forName("DeltaTable",spark)  
  .computeForAllColumns()  
```  

### Output:
The library will then compute the clustering metrics and generate a dataframe containing the next columns:  

| column   | total_file_count | total_uniform_file_count | average_overlap | average_overlap_depth | file_depth_histogram |
|----------|------------------|--------------------------|-----------------|-----------------------|----------------------|
| col_name | 5                | 5                        | 3.0             | 4 .0                  | {5.0 -> 0, 10.0 -... |  
  

```total_file_count```  
Total number of files composing the Delta table.

```total_uniform_file_count```  
Files in which min and max values of a given ordering column are equal

```average_overlap```  
Average number of overlapping files for each file in the delta table.  
The higher the average_overlap, the worse the clustering.

```average_overlap_depth```  
The average number of files that will be read when an overlap occurs.
The higher the average_overlap_depth, the worse the clustering.

```File_depth_histogram```  
A histogram detailing the distribution of the overlap_depth on the table by grouping the tables’ files by their proportional overlap depth.  
   * 0 to 16 with increments of 1.  
   * For buckets larger than 16, increments of twice the width of the previous bucket (e.g. 32, 64, 128, …)  

## NOTES
 
- Lighthouse cannot compute metrics for a column without statistics: Before computing clustering metrics, Lighthouse requires the statistics of the columns to be computed, so if statistics are not available, it will not be able to compute metrics for that column.  
- clustering metrics cannot be computed for partitioning columns  
- When handling a column with all null values, ```the average_overlap``` and ```average_overlap_depth``` metrics will be assigned a value of -1, while the ```file_depth_histogram``` metric will be assigned a null value.  

## LIMITATIONS
 
- Lighthouse currently supports the following data types: Int, Long, Decimal, and String.  
- Lighthouse supports only Delta tables and may not work with other table formats.  

## TECHNOLOGIES
 
Lighthouse supports:  
- Scala 2.12.13  
- Spark 3.3.2  
- Delta 2.3.0  

## CONTRIBUTING
 
Lighthouse is an open-source project, and we welcome contributors from the community. If you have a new feature or improvement, feel free to submit a pull request.  

## BLOGS

- [Z-ordering: take the Guesswork out (part1)](https://databeans-blogs.medium.com/z-ordre-take-the-guesswork-out-bad0133d7895)  
- [Z-ordering: take the Guesswork out (part2)](https://databeans-blogs.medium.com/delta-z-ordering-take-the-guesswork-out-part2-1bdd03121aec)

