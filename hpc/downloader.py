import ray
import requests
import json
import time

# Bootstrap our config file
CONFIG_PATH = 'downloader-config.json'
with open(CONFIG_PATH) as f:
  config = json.load(f)

# Use config file to setup our constants
API_KEY = config['api_key']
API_RESET = config['api_limit_reset_time']
API_QUANTITY = config['api_limit_quantity']
DOWNLOADS_PATH = config['downloads_path']
ENDPOINTS = config['glassnode_endpoints']

# Start ray
ray.init()


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

  print(f"Queueing up {API_QUANTITY} requests at {time.time()}")

  for i in range(API_QUANTITY):
    # DO RAY

  time_elapsed = time.time() - start_time
  time.sleep(API_RESET - time_elapsed)


r = requests.get(endpoint['url'],params=params)

with open(f"test/{coin}/{endpoint['name']}.json",'w') as f:
    f.write(r.text)




