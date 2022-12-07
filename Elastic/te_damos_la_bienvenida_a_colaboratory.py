import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import matplotlib.pyplot as plt
from seaborn import load_dataset
import datetime

ELASTIC_ID = "deployment_real_experience_001:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJGFlMzM3YWI2MmNlMTQ0YmFhMDIyMDFlNTUyZTNhNGMzJGEwNDk3MmY3Mzg4NjQzOWI4ZGVjMDZhYzFhMmU1YTA1"
KEY_ID = "EEtHnHoBOnRVzkpI3oRI"
MASTER = "6rwJg4lUQN2OXuFCzNDMMQ"

es = Elasticsearch(cloud_id=ELASTIC_ID, api_key=(KEY_ID, MASTER), timeout=30)

index = "speedtest_am*"

body = {
  "size": 8640, # 1 mes de datos
  "sort": { "@timestamp": "desc"},
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "@timestamp": {
        #      "gt": "2022-05-13T00:00:00.000+02:00"
              "lt": "now"
            }
          }
        },
        {
          "term": {
            "agent.hostname": "probe.aaaab.real.net.co"
          }
        }
      ]
    }
  }
}

res = es.search(index=index, body=body)

lista = []
for doc in res["hits"]["hits"]:
  lista.append(doc["_source"])

df = pd.DataFrame(lista)

df["column1"] = pd.to_datetime(df["column1"], format="%d-%m-%y-%H-%M-%S", utc=None)

df.fillna(method="ffill", inplace=True)
df.set_index("column1", inplace=True)

df = df.resample("1D").mean()
