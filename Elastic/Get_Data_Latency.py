# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 12:00:52 2022

@author: Jaider R
"""
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import matplotlib.pyplot as plt
import datetime

ELASTIC_ID = "deployment_real_experience_001:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJGFlMzM3YWI2MmNlMTQ0YmFhMDIyMDFlNTUyZTNhNGMzJGEwNDk3MmY3Mzg4NjQzOWI4ZGVjMDZhYzFhMmU1YTA1"
KEY_ID = "EEtHnHoBOnRVzkpI3oRI"
MASTER = "6rwJg4lUQN2OXuFCzNDMMQ"

es = Elasticsearch(cloud_id=ELASTIC_ID, api_key=(KEY_ID, MASTER), timeout=30)

index = "speedtest_am*","pfsense*"

body = {
  "size": 8640, # 25920:3 meses de datos, exactamente 90 dias. 17280: 2 meses
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

df_1d = df.resample("1D").mean()      # Separa los datos por dia
df_1df = df.resample("B").mean()      # Separa los datos por dias habiles
df_1h = df.resample("1H").mean()      # Separa los datos por hora
df = df.resample("5Min").mean()       # Separa los datos cada 5 minutos

#### Guardar en variables y añadir en grupos los indexes de fechas y el segmento solicitado ####

df_upload_index = df.groupby(["column1"])[["upload"]].mean()
df_latency_index = df.groupby(["column1"])[["latency"]].mean()
df_upload_bytes_index = df.groupby(["column1"])[["upload_bytes"]].mean()
df_packet_loss_index = df.groupby(["column1"])[["packet_loss"]].mean()
df_download_index = df.groupby(["column1"])[["download"]].mean()
df_jitter_index = df.groupby(["column1"])[["jitter"]].mean()
df_download_bytes_index = df.groupby(["column1"])[["download_bytes"]].mean()
df_uso_download_index = df.groupby(["column1"])[["pfsense_lan_traffic_download"]].mean()
df_uso_upload_index = df.groupby(["column1"])[["pfsense_lan_traffic_upload"]].mean()

#### Guardar en variables los grupos reseteando las columnas, dejando que "columns1" como se vuelva un index ####

df_upload = df_upload_index.reset_index()
df_latency = df_latency_index.reset_index()
df_upload_bytes = df_upload_bytes_index.reset_index()
df_packet_loss = df_packet_loss_index.reset_index()
df_download = df_download_index.reset_index()
df_jitter = df_jitter_index.reset_index()
df_download_bytes = df_download_bytes_index.reset_index()
df_uso_download = df_uso_download_index.reset_index()
df_uso_upload = df_uso_upload_index.reset_index()

#### Se crea una variable "days" para poder asignar a la nueva columna con nombres de la semana ####

days = {0:'Lunes',1:'Martes',2:'Miercoles',3:'Jueves',4:'Viernes',5:'Sabado',6:'Domingo'}

#### Se crea la nueva columna "semana" y se agrupan los datos de la variable "days" ####
# Columna para upload con dias de semana ####
df_upload['column1'] = pd.to_datetime(df_upload['column1'])
df_upload['semana'] = df_upload['column1'].dt.dayofweek
df_upload['semana'] = df_upload['semana'].apply(lambda x: days[x])
df_upload.set_index("column1", inplace=True)

# Columna para latency con dias de semana ####
df_latency['column1'] = pd.to_datetime(df_latency['column1'])
df_latency['semana'] = df_latency['column1'].dt.dayofweek
df_latency['semana'] = df_latency['semana'].apply(lambda x: days[x])
df_latency.set_index("column1", inplace=True)

# Columna para upload_bytes con dias de semana ####
df_upload_bytes['column1'] = pd.to_datetime(df_upload_bytes['column1'])
df_upload_bytes['semana'] = df_upload_bytes['column1'].dt.dayofweek
df_upload_bytes['semana'] = df_upload_bytes['semana'].apply(lambda x: days[x])
df_upload_bytes.set_index("column1", inplace=True)

# Columna para packet_loss con dias de semana ####
df_packet_loss['column1'] = pd.to_datetime(df_packet_loss['column1'])
df_packet_loss['semana'] = df_packet_loss['column1'].dt.dayofweek
df_packet_loss['semana'] = df_packet_loss['semana'].apply(lambda x: days[x])
df_packet_loss.set_index("column1", inplace=True)

# Columna para download con dias de semana ####
df_download['column1'] = pd.to_datetime(df_download['column1'])
df_download['semana'] = df_download['column1'].dt.dayofweek
df_download['semana'] = df_download['semana'].apply(lambda x: days[x])
df_download.set_index("column1", inplace=True)

# Columna para jitter con dias de semana ####
df_jitter['column1'] = pd.to_datetime(df_jitter['column1'])
df_jitter['semana'] = df_jitter['column1'].dt.dayofweek
df_jitter['semana'] = df_jitter['semana'].apply(lambda x: days[x])
df_jitter.set_index("column1", inplace=True)

# Columna para download_bytes con dias de semana ####
df_download_bytes['column1'] = pd.to_datetime(df_download_bytes['column1'])
df_download_bytes['semana'] = df_download_bytes['column1'].dt.dayofweek
df_download_bytes['semana'] = df_download_bytes['semana'].apply(lambda x: days[x])
df_download_bytes.set_index("column1", inplace=True)

# Columna para uso canal de bajada con dias de semana ####
df_uso_download['column1'] = pd.to_datetime(df_uso_download['column1'])
df_uso_download['semana'] = df_uso_download['column1'].dt.dayofweek
df_uso_download['semana'] = df_uso_download['semana'].apply(lambda x: days[x])
df_uso_download.set_index("column1", inplace=True)

# Columna para uso canal de subida con dias de semana ####
df_uso_upload['column1'] = pd.to_datetime(df_uso_upload['column1'])
df_uso_upload['semana'] = df_uso_upload['column1'].dt.dayofweek
df_uso_upload['semana'] = df_uso_upload['semana'].apply(lambda x: days[x])
df_uso_upload.set_index("column1", inplace=True)

# Upload: Capacidad de subida
df_upload_lunes = df_upload[df_upload.index.weekday.isin([0])]
df_upload_martes = df_upload[df_upload.index.weekday.isin([1])]
df_upload_miercoles = df_upload[df_upload.index.weekday.isin([2])]
df_upload_jueves = df_upload[df_upload.index.weekday.isin([3])]
df_upload_viernes = df_upload[df_upload.index.weekday.isin([4])]

# Latencia
df_latency_lunes = df_latency[df_latency.index.weekday.isin([0])]
df_latency_martes = df_latency[df_latency.index.weekday.isin([1])]
df_latency_miercoles = df_latency[df_latency.index.weekday.isin([2])]
df_latency_jueves = df_latency[df_latency.index.weekday.isin([3])]
df_latency_viernes = df_latency[df_latency.index.weekday.isin([4])]

# upload bytes
df_upload_bytes_lunes = df_upload_bytes[df_upload_bytes.index.weekday.isin([0])]
df_upload_bytes_martes = df_upload_bytes[df_upload_bytes.index.weekday.isin([1])]
df_upload_bytes_miercoles = df_upload_bytes[df_upload_bytes.index.weekday.isin([2])]
df_upload_bytes_jueves = df_upload_bytes[df_upload_bytes.index.weekday.isin([3])]
df_upload_bytes_viernes = df_upload_bytes[df_upload_bytes.index.weekday.isin([4])]

# Packet loss
df_packet_loss_lunes = df_packet_loss[df_packet_loss.index.weekday.isin([0])]
df_packet_loss_martes = df_packet_loss[df_packet_loss.index.weekday.isin([1])]
df_packet_loss_miercoles = df_packet_loss[df_packet_loss.index.weekday.isin([2])]
df_packet_loss_jueves = df_packet_loss[df_packet_loss.index.weekday.isin([3])]
df_packet_loss_viernes = df_packet_loss[df_packet_loss.index.weekday.isin([4])]

# Download: Capacidad de bajada
df_download_lunes = df_download[df_download.index.weekday.isin([0])]
df_download_martes = df_download[df_download.index.weekday.isin([1])]
df_download_miercoles = df_download[df_download.index.weekday.isin([2])]
df_download_jueves = df_download[df_download.index.weekday.isin([3])]
df_download_viernes = df_download[df_download.index.weekday.isin([4])]

# Jitter
df_jitter_lunes = df_jitter[df_jitter.index.weekday.isin([0])]
df_jitter_martes = df_jitter[df_jitter.index.weekday.isin([1])]
df_jitter_miercoles = df_jitter[df_jitter.index.weekday.isin([2])]
df_jitter_jueves = df_jitter[df_jitter.index.weekday.isin([3])]
df_jitter_viernes = df_jitter[df_jitter.index.weekday.isin([4])]

# Download bytes
df_download_bytes_lunes = df_download_bytes[df_download_bytes.index.weekday.isin([0])]
df_download_bytes_martes = df_download_bytes[df_download_bytes.index.weekday.isin([1])]
df_download_bytes_miercoles = df_download_bytes[df_download_bytes.index.weekday.isin([2])]
df_download_bytes_jueves = df_download_bytes[df_download_bytes.index.weekday.isin([3])]
df_download_bytes_viernes = df_download_bytes[df_download_bytes.index.weekday.isin([4])]

# Uso canal de bajada
df_uso_download_lunes = df_upload[df_upload.index.weekday.isin([0])]
df_uso_download_martes = df_upload[df_upload.index.weekday.isin([1])]
df_uso_download_miercoles = df_upload[df_upload.index.weekday.isin([2])]
df_uso_download_jueves = df_upload[df_upload.index.weekday.isin([3])]
df_uso_download_viernes = df_upload[df_upload.index.weekday.isin([4])]

# Uso canal de subida
df_uso_upload_lunes = df_upload[df_upload.index.weekday.isin([0])]
df_uso_upload_martes = df_upload[df_upload.index.weekday.isin([1])]
df_uso_upload_miercoles = df_upload[df_upload.index.weekday.isin([2])]
df_uso_upload_jueves = df_upload[df_upload.index.weekday.isin([3])]
df_uso_upload_viernes = df_upload[df_upload.index.weekday.isin([4])]