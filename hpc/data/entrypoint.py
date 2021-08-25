from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

import pyspark.sql.functions as F
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Spark session & context
conf = SparkConf()
conf.setAppName("final-project")

sc = SparkContext(conf=conf)
spark = SparkSession(sc)
# units are seconds
ts_bin_size = 60 * 60 * 24  # Round to nearest day

bitcoin_market_cap_DF = spark.read.csv("data/btc/market-cap.csv", inferSchema=True, header=True)
bitcoin_market_cap_DF = bitcoin_market_cap_DF.withColumn(
  'ts_bin',
  F.round(F.col('time') / ts_bin_size)
)

# saves to directory
bitcoin_market_cap_DF.coalesce(1).write.mode('overwrite').option('header','true').csv('hdfs:////user/sb7875/test-output/market-cap')
