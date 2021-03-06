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

all_coins = ['btc', 'eth', 'ltc']
prediction_coin = 'btc'
price_forecast_distance = -1 # NOTE WEIRD BUG THIS MUST STAY -1

class CoinPredictor:
  def __init__(self, ts_bin_size, all_coins, prediction_coin):
    self.ts_bin_size = ts_bin_size
    self.all_coins = all_coins
    self.prediction_coin = prediction_coin
  #
  #
  # Create a frame with a timestamp bin
  def createFrame(self, coin, file):
    df = spark.read.csv(f"data/{coin}/{file}", inferSchema=True, header=True)
    return df.withColumn(f'ts_bin', F.round(F.col('time') / self.ts_bin_size))
#
  def loadCoinData(self, coin, composite_df):
    df = self.createFrame(coin, "market-cap.csv").select('ts_bin', f'market_cap_{coin}')
    composite_df = composite_df.join(df, 'ts_bin', 'left_outer') #\
    #
    df = self.createFrame(coin, "transaction-count.csv").select('ts_bin', f'transaction_count_{coin}')
    composite_df = composite_df.join(df, 'ts_bin', 'left_outer') #\
    #
    # df = self.createFrame(coin, "price-ohlc.csv") \
    #   .select('ts_bin', f'open_price_{coin}', f'high_price_{coin}', f'low_price_{coin}', f'close_price_{coin}')
    # composite_df = composite_df.join(df, 'ts_bin', 'left_outer') #\
    #
    return composite_df
#
  def predict(self, price_forecast_distance):
    # Read in market-cap data
    composite_df = self.createFrame(self.prediction_coin, "price.csv") \
      .select('time', 'ts_bin', f'price_{self.prediction_coin}') #.sort(F.asc("time"))
    #
    # This is expensive, so we do it first, and then persist it.
    window = Window.orderBy(f'ts_bin')
    composite_df = composite_df.withColumn(f"price_forecast_{self.prediction_coin}", F.lag(f'price_{self.prediction_coin}', price_forecast_distance).over(window))
    composite_df.persist()
    composite_df.show()
    #
    for coin in self.all_coins:
      composite_df = self.loadCoinData(coin, composite_df)
#
#
    composite_df.show()
#
    composite_df.coalesce(1).write.mode('overwrite').option('header','true').csv('hdfs:///user/sb7875/output/combined_csv_data')
#
    inputCols = ["ts_bin"]
#
    for coin in self.all_coins:
      inputCols.append(f"market_cap_{coin}")
      inputCols.append(f"transaction_count_{coin}")
#
    # We can't use rows with nulls
    composite_df = composite_df.dropna('any')
#
    vector_assembler = VectorAssembler(inputCols=inputCols, outputCol='VFeatures', handleInvalid='skip')
    output = vector_assembler.transform(composite_df)
    output.limit(2).show()
#
    traindata, testdata = output.randomSplit([0.75, 0.25])
    regressor = LinearRegression(featuresCol='VFeatures', labelCol=f'price_forecast_{self.prediction_coin}')
    regressor = regressor.fit(traindata)
    pred = regressor.evaluate(testdata)
#
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
#
    pred.predictions.select('ts_bin', f'price_{self.prediction_coin}', f'price_forecast_{self.prediction_coin}') \
      .coalesce(1).write.mode('overwrite').option('header','true') \
      .csv(f'hdfs:///user/sb7875/output/{price_forecast_distance * -1}_day_predictions')


c = CoinPredictor(ts_bin_size, all_coins, prediction_coin)
c.predict(price_forecast_distance)

