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

with open('downloader-config.json') as f:
  config = json.load(f)

# units are seconds
ts_bin_size = config['ts_bin_size']
all_coints = config['coins']
prediction_coin = config['prediction_coin']
price_forecast_distance = config['price_forecast_distance']
metrics = map(lambda endpoint: endpoint['name'], config['endpoints'])

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
    for metric in metrics:
      df = self.createFrame(coin, f"{metric}.csv").select('ts_bin', f'{metric}_{coin}')
      composite_df = composite_df.join(df, 'ts_bin', 'left_outer') #\

    return composite_df
#
  def predict(self, price_forecast_distance):
    # Read in market-cap data
    composite_df = self.createFrame(self.prediction_coin, "price.csv") \
      .select('time', 'ts_bin', f'price_{self.prediction_coin}') #.sort(F.asc("time"))
    #
    # This is expensive, so we do it first, and then persist it.
    window = Window.orderBy(f'ts_bin')
    composite_df = composite_df.withColumn(f"price_forecast_{self.prediction_coin}", F.lag(f'price_{self.prediction_coin}', price_forecast_distance * -1).over(window))
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
      for metric in metrics:
        inputCols.append(f"{metrics}_{coin}")
#
    # We can't use rows with nulls
    composite_df = composite_df.dropna('any')
#
    vector_assembler = VectorAssembler(inputCols=inputCols, outputCol='VFeatures', handleInvalid='skip')
    output = vector_assembler.transform(composite_df)
    output.limit(2).show()


    test_data = output.sort(F.desc("ts_bin")).limit(price_forecast_distance)
    traindata = output.subtract(test_data)
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
      .csv(f'hdfs:///user/sb7875/output/{price_forecast_distance}_day_predictions')


c = CoinPredictor(ts_bin_size, all_coins, prediction_coin)
c.predict(price_forecast_distance)

