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
prediction_coin = 'btc'
price_forecast_distance = -1





###### NOTE CONSIDER DOING DISTINCT BY BIN
###### NOTE CONSIDER A PARITION KEY, LIKE MONTH OR QUARTER
###### NOTE WINDOWING SHOULD BE DONE FIRST, WHILE THE DATA IS STILL SMALL


# Create a frame with a timestamp bin
def createFrame(coin, file):
  df = spark.read.csv(f"data/{coin}/{file}", inferSchema=True, header=True)
  return df.withColumn(f'ts_bin', F.round(F.col('time') / ts_bin_size))

def loadCoinData(coin, composite_df):
  df = createFrame(coin, "market-cap.csv").select('ts_bin', f'market_cap_{coin}')
  composite_df = composite_df.join(df, 'ts_bin', 'left_outer') #\
  # .select(
  #   composite_df.time,
  #   composite_df.ts_bin,
  #   F.col(f"market_cap_{coin}"),
  #   composite_df.price,
  #   composite_df.price_forecast,
  # )
  #
  df = createFrame(coin, "transaction-count.csv").select('ts_bin', f'transaction_count_{coin}')
  composite_df = composite_df.join(df, 'ts_bin', 'left_outer') #\
  # .select(
  #   composite_df.time,
  #   composite_df.ts_bin,
  #   F.col(f"market_cap_{coin}"),
  #   F.col(f"transaction_count_{coin}"),
  #   composite_df.price,
  #   composite_df.price_forecast,
  # )
  #
  return composite_df


# Read in market-cap data
composite_df = createFrame(prediction_coin, "price.csv") \
  .select('time', 'ts_bin', f'price_{prediction_coin}') #.sort(F.asc("time"))

#
# This is expensive, so we do it first, and then persist it.
window = Window.orderBy(f'ts_bin')
composite_df = composite_df.withColumn(f"price_forecast_{prediction_coin}", F.lag(f'price_{prediction_coin}', price_forecast_distance).over(window))
composite_df.persist()
composite_df.show()

composite_df = loadCoinData('btc', composite_df)
composite_df = loadCoinData('eth', composite_df)
composite_df = loadCoinData('ltc', composite_df)
composite_df.show()


# saves to directory
composite_df.coalesce(1).write.mode('overwrite').option('header','true').csv('hdfs:///user/sb7875/test-output/combined_csv_data')



# # Start Machine Learning!
inputCols=[
  "ts_bin",
  "market_cap_btc",
  "transaction_count_btc",
  "market_cap_eth",
  "transaction_count_eth",
  "market_cap_ltc",
  "transaction_count_ltc"
]
feature_assembler = VectorAssembler(inputCols=inputCols, outputCol='VFeatures', handleInvalid='skip')
output = feature_assembler.transform(composite_df)
output.limit(2).show()


traindata, testdata = output.randomSplit([0.75, 0.25])
regressor = LinearRegression(featuresCol='VFeatures', labelCol=f'price_forecast_{prediction_coin}')
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

pred.predictions.select(f'price_{prediction_coin}', f'price_forecast_{prediction_coin}') \
  .coalesce(1).write.mode('overwrite').option('header','true') \
  .csv(f'hdfs:///user/sb7875/output/{price_forecast_distance * -1}_day_predictions')
