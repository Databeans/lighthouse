# DeltaClusteringMetrics Example
This example demonstrates how to use DeltaClusteringMetrics to analyze the data layout of a Delta table.  
DeltaClusteringMetrics is an open-source project designed to monitor the health of Delta tables from a data layout perspective. It provides valuable insights into how data is distributed inside parquet files that make up the Delta table.   
This example calculates the clustering metrics using DeltaClusteringMetrics of delta table, and prints the results to the console. It can be run if the prerequisites are satisfied.

## Prerequisites  
- Scala 2.12.13
- Spark 3.2.0
- Delta 2.0.0  
- clusteringinfo_2.12-0.1.1.jar  

## Instructions  
To run the example:  
1. Download or clone the DeltaClusteringMetrics project.  
2. Navigate to the examples directory: ```cd examples```.  
3. Copy the clusteringinfo_2.12-0.1.1.jar file to the lib directory.  
4. Run ```sbt compile``` to compile the example.  
5. Run ```sbt run``` to execute the example.  
6. The clustering metrics for the specified Delta table will be printed to the console.    

By running this example, you can learn how to use DeltaClusteringMetrics to calculate the clustering metrics for a Delta table and interpret the results.  
You can also use this example as a starting point for your own DeltaClusteringMetrics projects.



