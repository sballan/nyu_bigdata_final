import requests
import json

API_KEY = "1wYkOLcQYMhggGz8fIJKo1rTDch"
ENDPOINTS_PATH = 'glassnode-endpoints.json'
DOWNLOADS_PATH = 'data/json'

with open('glassnode-endpoints.json') as f:
  endpoints = json.load(f)


coin = 'btc'
endpoint = endpoints[0]

params = {
  'api_key': API_KEY,
  'a': coin,
}

params = endpoint["params"]
params['api_key'] = API_KEY
params['a'] = coin

r = requests.get(endpoint['url'],params=params)

with open(f"test/{coin}/{endpoint['name']}.json",'w') as f:
    f.write(r.text)




