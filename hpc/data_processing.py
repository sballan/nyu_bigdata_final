import json
import csv
from datetime import datetime

def processData(coin):
  # ---- PRICE
  jsonfile = open(f"data/json/{coin}/price.json")
  bitcoin_price_json = json.load(jsonfile)
  #
  csvfile = open(f"data/{coin}/price.csv", 'w')
  writer = csv.writer(csvfile)
  writer.writerow(['time', f'price_{coin}'])
  #
  for row in bitcoin_price_json:
    t = int(datetime.strptime(row['t'],"%Y-%m-%dT%H:%M:%SZ").timestamp())
    writer.writerow([t, row['v']])
  #
  csvfile.close()
  jsonfile.close()
  #
  # ---- OHLC
  jsonfile = open(f"data/json/{coin}/price-ohlc.json")
  price_ohlc_json = json.load(jsonfile)
  #
  csvfile = open(f"data/{coin}/price-ohlc.csv", 'w')
  writer = csv.writer(csvfile)
  writer.writerow(['time', f"open_price_{coin}", f'high_price_{coin}', f'low_price_{coin}', f'close_price_{coin}'])
  #
  for row in price_ohlc_json:
    t = int(datetime.strptime(row['t'],"%Y-%m-%dT%H:%M:%SZ").timestamp())
    writer.writerow([t, row['o']['o'], row['o']['h'], row['o']['l'], row['o']['c']])
  #
  csvfile.close()
  jsonfile.close()
  #
  # ---- MARKET-CAP
  filename = "market-cap"
  jsonfile = open(f"data/json/{coin}/%s.json" % filename)
  json_data = json.load(jsonfile)
  #
  csvfile = open(f"data/{coin}/%s.csv" % filename, 'w')
  writer = csv.writer(csvfile)
  writer.writerow(['time', f'market_cap_{coin}'])
  #
  for row in json_data:
    t = int(datetime.strptime(row['t'],"%Y-%m-%dT%H:%M:%SZ").timestamp())
    writer.writerow([t, row['v']])
  #
  csvfile.close()
  jsonfile.close()
  #
  # ---- TRANSACTION-COUNT
  filename = "transaction-count"
  jsonfile = open(f"data/json/{coin}/%s.json" % filename)
  json_data = json.load(jsonfile)

  csvfile = open(f"data/{coin}/%s.csv" % filename, 'w')
  writer = csv.writer(csvfile)
  writer.writerow(['time', f'transaction_count_{coin}'])

  for row in json_data:
      t = int(datetime.strptime(row['t'],"%Y-%m-%dT%H:%M:%SZ").timestamp())
      writer.writerow([t, row['v']])

  csvfile.close()
  jsonfile.close()


processData('btc')
processData('eth')
processData('ltc')


