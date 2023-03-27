# DeltaClusteringMetrics

## OVERVIEW
___ 
DeltaClusteringMetrics is an open-source project that aims to analyze an existing delta table and provide an overview of it in terms of performance and cost.   
The tool detects several metrics including:  
- Total_file_count:  
This metric computes the total number of files composing the delta table.  
- Total_uniform_file_count:  
This metrics computes files in which min and max values of a given ordering column are equal.  
- Average_overlap for each table:  
This metric computes the average number of overlapping files for each file in the delta table.  
- Average_overlap_depth for each table:  
This metric computes the average number of files that will be read when an overlap occurs.  
- File_depth_histogram for each table:  
This metric provides a histogram detailing the distribution of the overlap depth on the table by grouping the table’s files by their proportional overlap depth.  
The main goal of DeltaClusteringMetrics is to improve the users experience by providing insights into the performance and cost of the delta lake table.  
By using this tool, users can optimize their data pipelines and make informed decisions on how to manage their data more effectively and could analyze the performance of executed queries on each table.

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

