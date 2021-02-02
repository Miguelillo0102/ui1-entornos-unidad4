from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("Example script").getOrCreate()
rdd1=spark.sparkContext.parallelize( [x for x in range(1000000) ] )
rdd1.saveAsTextFile('example_result')
