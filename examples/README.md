# DeltaClusteringMetrics Example
This example demonstrates how to use DeltaClusteringMetrics to analyze the data layout of a Delta table.  
DeltaClusteringMetrics is library designed to monitor the health of the Lakehouse from a data layout perspective, and provides valuable insights about how well the data is clustered.   
This example calculates the clustering metrics using DeltaClusteringMetrics of delta table, and prints the results to the console. It can be run if the prerequisites are satisfied.

## Prerequisites  
- Scala 2.12.13
- Spark 3.2.0
- Delta 2.0.0  
- clusteringinfo_2.12-0.1.1.jar  

## Instructions  
To run the example:  
1. Download or clone the DeltaClusteringMetrics project.   
2. run ```sbt build``` then ```sbt compile```.  
3. run ```sbt package``` to generate the jar file.  
4. run ```cp target/scala-2.12/clusteringinfo_2.12-0.1.0.jar examples/lib/``` to copy the jar in the lib folder.  
5. Navigate to the examples directory: ```cd examples```.  
6. Run ```sbt compile``` to compile the example.  
7. Run ```sbt "runMain Quickstart --master local[*]"``` to execute the example.  
8. The clustering metrics for the specified Delta table will be printed to the console.    

By running this example, you can learn how to use DeltaClusteringMetrics to calculate the clustering metrics for a Delta table and interpret the results.  
You can also use this example as a starting point for your own DeltaClusteringMetrics projects.



