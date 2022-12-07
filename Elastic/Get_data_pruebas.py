import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import matplotlib.pyplot as plt
import datetime
import seaborn as sns

ELASTIC_ID = "deployment_real_experience_001:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJGFlMzM3YWI2MmNlMTQ0YmFhMDIyMDFlNTUyZTNhNGMzJGEwNDk3MmY3Mzg4NjQzOWI4ZGVjMDZhYzFhMmU1YTA1"
KEY_ID = "EEtHnHoBOnRVzkpI3oRI"
MASTER = "6rwJg4lUQN2OXuFCzNDMMQ"

es = Elasticsearch(cloud_id=ELASTIC_ID, api_key=(KEY_ID, MASTER), timeout=30)

index = "speedtest*", "pfsense*"

body = {
  "size": 25920, ### 25920:3 meses de datos ### 17280: 2 meses de datos ### 8640: 1 mes de datos
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
            "agent.hostname": "probe.aaaab.real.net.co" #La caja a consultar los datos
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

######### Convertir en index la columna de fecha ###############################

df["column1"] = pd.to_datetime(df["column1"], format="%d-%m-%y-%H-%M-%S", utc=None)
df.fillna(method="ffill", inplace=True)
df.set_index("column1", inplace=True)

######### Recoger datos en intervalos de 5 minutos #############################

df = df.resample("5Min").mean()       # Separa los datos cada 5 minutos

######### Funcion para calcular la mediana ######################################

def median(dataset):
    data = sorted(dataset)
    index = len(data) // 2
    # Conjunto de datos impar
    if len(dataset) % 2 != 0:
        return data[index]
    #Conjunto de datos par
    return (data[index - 1] + data[index]) / 2

days = {0:'Lunes',1:'Martes',2:'Miercoles',3:'Jueves',4:'Viernes',5:'Sabado',6:'Domingo'}

df = df.reset_index()
df['column1'] = pd.to_datetime(df['column1'])
df['day_name'] = df['column1'].dt.dayofweek
df['day_name'] = df['day_name'].apply(lambda x: days[x])

df_latency = df.groupby("column1")[["latency"]].agg([min, max, median])

df_latency_lunes = df_latency[df_latency.index.weekday.isin([0])]

df_latency_lunes_1H = df_latency_lunes.resample("1H").mean().dropna()
df_latency_lunes_1D = df_latency_lunes.resample("1D").mean().dropna()

min = df_latency_lunes["latency","min"]
med = df_latency_lunes["latency","median"]
max = df_latency_lunes["latency","max"]

minmin = min.rolling(25).min()
medmed = min.rolling(50).mean()
maxmax = max.rolling(75).max()

plt.figure(figsize=(20,10))

plt.subplot(311)
plt.title("Por 5 minutos")
plt.plot(min, label="min", color="orange")
plt.plot(med, label="med", color="black")
plt.plot(max, label="max", color="blue")
plt.plot(minmin, label="prueba_min", color="yellow")
plt.plot(maxmax, label="prueba_max", color="cyan")
plt.plot(medmed, label="prueba_med", color="lightgreen")
plt.legend()

plt.subplot(312)
plt.title("Por 1 Hora")
plt.plot(df_latency_lunes_1H)
plt.legend()

plt.subplot(313)
plt.title("Por 1 Dia")
plt.plot(df_latency_lunes_1D, label="min", color="green")
plt.legend()


plt.tight_layout()
plt.show()