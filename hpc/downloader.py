import json
import time
import os

import ray
import requests


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

print("Loaded Configs")

# Start ray
ray.init()

@ray.remote
def exec_request(req):
  req = json.loads(req)
  endpoint = req['endpoint']
  params = req['params']
  coin = params['a']

  try:
    # Automatically create necessary folders for this
    filename = f"{DOWNLOADS_PATH}/{coin}/{endpoint['name']}.json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    r = requests.get(endpoint['url'],params=params)
    with open(f"{filename}.json",'w') as f:
      f.write(r.text)
    return [True, coin, endpoint['url']]
  except:
    return [False, coin, endpoint['url']]


reqs = []
# Next, iterate over endpoints and coins
for coin in config['coins']:
  for endpoint in ENDPOINTS:
    params = endpoint['params']
    params['api_key'] = API_KEY
    params['a'] = coin

    req = {
      "params": params,
      "endpoint": endpoint
    }

    reqs.append(json.dumps(req))

print(f"Preparing {len(reqs)} requests across {len(config['coins'])} coins and {len(ENDPOINTS)} endpoints")


while len(reqs) > 0:
  num_reqs = min(len(reqs),API_QUANTITY)
  print(f"Queueing up {num_reqs} requests at {time.time()}")

  start_time = time.time()

  futures = [exec_request.remote(reqs.pop()) for i in range(num_reqs)]

  time_elapsed = time.time() - start_time
  print(f"We queued {num_reqs} in {time_elapsed} seconds.")
  print(f"About to sleep for {API_RESET - time_elapsed} seconds.")

  time.sleep(API_RESET - time_elapsed)

  print(ray.get(futures)) # [0, 1, 4, 9]






