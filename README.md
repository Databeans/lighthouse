# DeltaClusteringMetrics

## OVERVIEW
___ 
DeltaClusteringMetrics is an open-source project designed to analyze existing Delta tables and provide an overview of their data layout.  
Its primary objective is to enhance user experience by allowing them to assess query performance without running them and to identify when data maintenance operations should be performed on the table.  
By analyzing the data layout, the project provides valuable insights into how the data is distributed within the Delta table and how it can be optimized to improve query performance. These metrics help data engineers and analysts make informed decisions about data maintenance operations, such as vacuuming or compacting, which can improve query performance and reduce storage costs.  
Overall, DeltaClusteringMetrics is a powerful tool that helps Delta table users optimize their data for better performance and cost-effectiveness.  

## CLUSTERING METRICS  
---
DeltaClusteringMetrics detects several metrics including:  
### - total_file_count:  
Total number of files composing the Delta table.  
### - total_uniform_file_count:  
Files in which min and max values of a given ordering column are equal  
![](https://bitbucket.org/data_beans/clustering-info/src/user-guide/images/total_uniform_file_count.png)
### - average_overlap:  
Average number of overlapping files for each file in the delta table.  
Best case scenario: average_overlaps = 0 ⇒ table perfectly clustered with no overlapping files.   
Worst case scenario: average_overlaps = (total_file_count — 1) ⇒ table with all files overlapping.  
⇒ The higher the average_overlap, the worse the clustering.   
So to better illustrate, this is a simple example of a table consisting of 4 files:  
![](https://bitbucket.org/data_beans/clustering-info/src/user-guide/images/average_overlap.jpg)  
### - average_overlap_depth:  
The average number of files that will be read when an overlap occurs.   
Empty table ⇒ average_overlap_depth = 0   
Table with no overlapping files ⇒ average_overlap_depth = 1   
⇒ The higher the average_overlap_depth, the worse the clustering.   
Throughout this figure, we will study the evolution of the average_overlap_depth of a table containing 4 files:
![](https://bitbucket.org/data_beans/clustering-info/src/user-guide/images/average_overlap_depth.jpg)  
Initially, there are no overlapping files, so the table is perfectly clustered.   
⇒ Best case scenario: average_overlap_depth = 1.   
Going on, as the clustering is getting worse, the average_overlap_depth is getting higher.   
In the final stage, all files overlap in their entirety   
⇒ Worst case scenario: average_overlap_depth = number of files.  
### - File_depth_histogram:
A histogram detailing the distribution of the overlap_depth on the table by grouping the tables’ files by their proportional overlap depth.  
The histogram contains buckets with widths:
* 0 to 16 with increments of 1.  
* For buckets larger than 16, increments of twice the width of the previous bucket (e.g. 32, 64, 128, …)  

## HOW TO USE
___ 
To use the DeltaClusteringMetrics, simply provide the path or the name of your table and the columns that you want to extract its information as input parameters.
### Syntax
___ 
```
val clusteringMetrics = DeltaClusteringMetrics
.forPath("tablePath")
.computeForColumns("col1","col2")
```
--- 
```
val clusteringMetrics = DeltaClusteringMetrics
.forName("tableName")
.computeForColumns("columnName")
```
### Parameters
___ 
- forName(“ tableName ”): Name of the Delta Table.  
- forPath(“ Path ”): Path for the Delta Table.  
- computeForColumn(“columnName”): extract clustering information for a certain column.  
- computeForColumns(“col1”,”col2”,…): extract clustering information for multiple columns.  
- computeForAllColumns(): extract clustering information for the entire table.  
### Output
___ 
The tool will then extract the clustering metrics and generate a dataframe containing the next columns:  
- column  
- total_file_count  
- total_uniform_file_count  
- average_overlap  
- average_overlap_depth  
- file_depth_histogram  
### Example 
Let's look at this example
```
spark.range(1, 5, 1).toDF()
.withColumn("id", col("id").cast(IntegerType))
.withColumn("keys", lit(1))
.withColumn("values", col("id") * 3)
.write.format("delta")
.mode("overwrite").saveAsTable("DeltaTable")
```

| id  | keys | values |
|-----|------|--------|
| 1   | 1    | 3      |
| 2   | 1    | 6      |
| 3   | 1    | 9      |
| 4   | 1    | 12     |

Extract clustering information for one column:    
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  .computeForColumn("keys")
```
The result will be this dataframe  

| column | total_file_count | total_uniform_file_count | average_overlap | average_overlap_depth | file_depth_histogram                                                                                                                                                    |
|--------|------------------|--------------------------|-----------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Keys   | 4                | 4                        | 3               | 4                     | {"6.0": 0, "12.0": 0, "15.0": 0, "3.0": 0, "8.0": 0, "10.0": 0, "2.0": 0, "16.0": 0, "11.0": 0, "4.0": 4, "13.0": 0, "1.0": 0, "9.0": 0, "14.0": 0, "7.0": 0, "5.0": 0} |  
---  
Extract clustering information for one column:  
```
val clusteringMetrics = DeltaClusteringMetrics
  .forName("DeltaTable")
  ..computeForAllColumns()
```
The result will be this dataframe  

| column | total_file_count | total_uniform_file_count | average_overlap | average_overlap_depth | file_depth_histogram                                                                                                                                                    |
|--------|------------------|--------------------------|-----------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id     | 4                | 4                        | 0               | 1                     | {"6.0": 0, "12.0": 0, "15.0": 0, "3.0": 0, "8.0": 0, "10.0": 0, "2.0": 0, "16.0": 0, "11.0": 0, "4.0": 0, "13.0": 0, "1.0": 4, "9.0": 0, "14.0": 0, "7.0": 0, "5.0": 0} |
| keys   | 4                | 4                        | 3               | 4                     | {"6.0": 0, "12.0": 0, "15.0": 0, "3.0": 0, "8.0": 0, "10.0": 0, "2.0": 0, "16.0": 0, "11.0": 0, "4.0": 4, "13.0": 0, "1.0": 0, "9.0": 0, "14.0": 0, "7.0": 0, "5.0": 0} |
| values | 4                | 4                        | 0               | 1                     | {"6.0": 0, "12.0": 0, "15.0": 0, "3.0": 0, "8.0": 0, "10.0": 0, "2.0": 0, "16.0": 0, "11.0": 0, "4.0": 0, "13.0": 0, "1.0": 4, "9.0": 0, "14.0": 0, "7.0": 0, "5.0": 0} |  

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
- [Z-ordering: take the Guesswork out (part1)](https://databeans-blogs.medium.com/delta-z-ordering-take-the-guesswork-out-part2-1bdd03121aec)

