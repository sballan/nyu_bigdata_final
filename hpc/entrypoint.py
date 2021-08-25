from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf

import pyspark.sql.functions as F
from pyspark.sql.window import Window

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

def createFrame(path):
  df = spark.read.csv(path, inferSchema=True, header=True)
  return df.withColumn('ts_bin', F.round(F.col('time') / ts_bin_size))

# Read in market-cap data
bitcoin_market_cap_DF = spark.read.csv("data/btc/market-cap.csv", inferSchema=True, header=True)
bitcoin_market_cap_DF = bitcoin_market_cap_DF.withColumn(
  'ts_bin',
  F.round(F.col('time') / ts_bin_size)
)

# Read in transaction-count data
bitcoin_transaction_count_DF = spark.read.csv("data/btc/transaction-count.csv", inferSchema=True, header=True)
bitcoin_transaction_count_DF = bitcoin_transaction_count_DF.withColumn(
  'ts_bin',
  F.round(F.col('time') / ts_bin_size)
)
bitcoin_transaction_count_DF.sort(F.desc('time')).limit(1).show()

# Read in price data
bitcoin_price_DF = spark.read.csv("data/btc/price.csv", inferSchema=True, header=True)
bitcoin_price_DF = bitcoin_price_DF.withColumn(
  'ts_bin',
  F.round(F.col('time') / ts_bin_size)
)
bitcoin_price_DF.show()


# Combine these tables together
combined = bitcoin_price_DF.join(bitcoin_transaction_count_DF, bitcoin_price_DF.ts_bin == bitcoin_transaction_count_DF.ts_bin, 'outer') \
  .select(
    bitcoin_price_DF.ts_bin,
    bitcoin_price_DF.time,
    bitcoin_transaction_count_DF.transaction_count,
    bitcoin_price_DF.price
    ) \
  .sort(F.desc("time"))
combined.show()

window = Window.orderBy('ts_bin')
bitcoin_price_DF.withColumn("tom_price", F.lag("price", 1).over(window)).show()

combined = combined.join(bitcoin_market_cap_DF, combined.ts_bin == bitcoin_market_cap_DF.ts_bin, 'outer') \
  .select(
    combined.ts_bin,
    combined.time,
    combined.transaction_count,
    bitcoin_market_cap_DF.market_cap,
    combined.price
    ) \
  .sort(F.desc("time"))
combined.show()


# saves to directory
combined.coalesce(1).write.mode('overwrite').option('header','true').csv('hdfs:///user/sb7875/output/combined_csv_data')



# Start Machine Learning!
feature_assembler = VectorAssembler(inputCols=["time", "market_cap", "transaction_count"], outputCol='VFeatures', handleInvalid='skip')
output = feature_assembler.transform(combined)
output.limit(2).show()


traindata, testdata = output.randomSplit([0.75, 0.25])
regressor = LinearRegression(featuresCol='VFeatures', labelCol='price')
regressor = regressor.fit(traindata)

pred = regressor.evaluate(testdata)
print("""
  Features Column: %s
  Label Column: %s
  Explained Variance: %s
  r Squared %s
  r Squared (adjusted) %s
""" % (
  pred.featuresCol,
  pred.labelCol,
  pred.explainedVariance,
  pred.r2,
  pred.r2adj
))

pred.predictions.select('price', 'prediction').coalesce(1).write.mode('overwrite').option('header','true').csv('hdfs:///user/sb7875/output/0_day_predictions')
