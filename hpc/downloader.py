import requests
import json
import time

API_KEY = "1wYkOLcQYMhggGz8fIJKo1rTDch"
CONFIG_PATH = 'downloader-config.json'
DOWNLOADS_PATH = 'data/json'

# First get the config file
with open(CONFIG_PATH) as f:
  config = json.load(f)

API_RESET = config['api_limit_reset_time']
API_QUANTITY = config['api_limit_quantity']

request_params = []
# Next, iterate over endpoints and coins
for coin in config['coins']:
  for endpoint in endpoints:
    params = endpoints['params']
    params['api_key'] = API_KEY
    params['a'] = coin

    request_params.append(params)


while len(request_params) > 0:
  start_time = time.time()
  for i in range(30):
    # DO RAY

  time_elapsed = time.time() - start_time
  time.sleep(API_RESET - time_elapsed)


r = requests.get(endpoint['url'],params=params)

with open(f"test/{coin}/{endpoint['name']}.json",'w') as f:
    f.write(r.text)




