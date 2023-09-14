# Streaming-Spark-Databricks
- Read data as streaming data
- use different triggers while writing data sush as fixed interval & once micro batch
- Read data in Auto Loader format able incrementally process the data
- Make Transformation on data & use also spark.sql
  the transformation you make on batch data, also the same on stream data
- Writing output data with different modes: append, complete, and update
- Read and process data in AWS S3 buckets from Spark Databricks
- Use checkpoint while writing data to save intermediate results from failure

  ## Conclusion
  in Streaming Data Spark 2.X:
  - Every data item is arriving on stream is like new record being append to the input table
    Data stream are unbounded in input table
  - All operations can be performed in dataframe can also done in streaming data
