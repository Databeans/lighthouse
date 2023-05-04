# lighthouse Example
This example demonstrates how to use lighthouse to analyze the data layout of a Delta table.  
lighthouse is a library designed to monitor the health of the Lakehouse from a data layout perspective, and provide valuable insights about how well data is clustered.   
This example calculates the clustering metrics of a delta table, and prints the results to the console. It can be run if the prerequisites are satisfied.

## Prerequisites  
- Scala 2.12.13
- Spark 3.3.2
- Delta 2.3.0  
- lighthouse_2.12-0.1.0.jar  

## Instructions  
To run the example:  
1. Download or clone the lighthouse project.   
2. run ```sbt build``` then ```sbt compile```.  
3. run ```sbt package``` to generate the jar file.
4. run ```mkdir examples/lib/ ``` to create the lib directory.  
5. run ```cp target/scala-2.12/lighthouse_2.12-0.1.0.jar examples/lib/``` to copy the jar in the lib folder.  
6. Navigate to the examples directory: ```cd examples```.  
7. Run ```sbt compile``` to compile the example.  
8. Run ```sbt "runMain Quickstart --master local[*]"``` to execute the example.  
9. The clustering metrics for the specified Delta table will be printed to the console.    

By running this example, you can learn how to use lighthouse to calculate the clustering metrics for a Delta table and interpret the results.  
You can also use this example as a starting point for your own projects.



