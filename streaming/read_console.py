from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("kafka-console-tap")
         .config("spark.sql.shuffle.partitions","2")
         .getOrCreate())

df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","kafka:9092")
      .option("subscribe","events.raw")
      .option("startingOffsets","latest")
      .load())

parsed = df.selectExpr("CAST(key AS STRING) AS k", "CAST(value AS STRING) AS v", "timestamp AS ts")

q = (parsed.writeStream
     .format("console")
     .option("truncate","false")
     .outputMode("append")
     .start())

spark.streams.awaitAnyTermination()
