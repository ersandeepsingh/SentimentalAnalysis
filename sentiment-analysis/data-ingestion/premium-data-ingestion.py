import requests
import json

endpoint = "https://api.twitter.com/1.1/tweets/search/30day/dev.json" 

headers = {"Authorization":"Bearer AAAAAAAAAAAAAAAAAAAAAG9A9wAAAAAAevQMBqTqDUMGADX8rJG12GHDSjM%3DQasp1y1gpuioAYmDcI8HDEAbCENonWxEttW9JjyzatxgcKbgTT", "Content-Type": "application/json"}  

data = '{"query":"saudivision2030", "fromDate": "201904020000", "toDate": "201904150000"}'

response = requests.post(endpoint,data=data,headers=headers).json()
print(json.dumps(response, indent = 2))
print(len(response))
