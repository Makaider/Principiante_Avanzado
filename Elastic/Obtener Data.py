# -*- coding: utf-8 -*-
"""
Created on Tue Jun 28 10:45:24 2022

@author: Daniel
"""

from elasticsearch import Elasticsearch, helpers
import logging
import traceback

# Credenciales Clúster nuevo #################################################
ELASTIC_ID = "deployment_tests_real_experience:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo0NDMkMzRiN2I5MGM5YWJjNDQ1Nzg0N2ViM2ZmNWZkNmI3N2UkOTVhZDVhYmU5NzY3NGVjNGE1ZGQ0MWZjNWFlOGZjMmQ="
# {"id":"THoKq4EBRhQifaF5KZIE","name":"api para extracción","api_key":"qwVtxF9nQMWM-Lekitcsfw","encoded":"VEhvS3E0RUJSaFFpZmFGNUtaSUU6cXdWdHhGOW5RTVdNLUxla2l0Y3Nmdw=="}
# VEhvS3E0RUJSaFFpZmFGNUtaSUU6cXdWdHhGOW5RTVdNLUxla2l0Y3Nmdw==
KEY_ID = "THoKq4EBRhQifaF5KZIE"
MASTER = "qwVtxF9nQMWM-Lekitcsfw"

# Credenciales CLúster principal :D ##########################################
ELASTIC_ID_Main = "deployment_real_experience_001:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJGFlMzM3YWI2MmNlMTQ0YmFhMDIyMDFlNTUyZTNhNGMzJGEwNDk3MmY3Mzg4NjQzOWI4ZGVjMDZhYzFhMmU1YTA1"

KEY_ID_Main = "EEtHnHoBOnRVzkpI3oRI"
MASTER_Main = "6rwJg4lUQN2OXuFCzNDMMQ"


# Activación clientes Elasticsearch ##########################################
es = Elasticsearch(cloud_id=ELASTIC_ID, api_key=(KEY_ID, MASTER), timeout=30) # Requerimiento
es_main = Elasticsearch(cloud_id=ELASTIC_ID_Main, api_key=(KEY_ID_Main, MASTER_Main), timeout=30) # Principal

# Nombre del índice ##########################################################
index_name = "logs-elastic" # Nombre de índice

# Logging ####################################################################
logger = logging.getLogger('LOG-ELASTIC')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('logs-elastic.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
fh.setFormatter(formatter)

# # ENVIAR DATA ("Bulkear") ##################################################

def generate_docs(reader):
    for row in reader:
        doc = {
            "_index": index_name,
            "_id": row["_id"],
            "_source": {
                "id": row["_id"],
                "@timestamp": row["_source"]["@timestamp"],
                "loglevel": row["_source"]["log"]["level"],
                "dataset": row["_source"]["event"]["dataset"],
                "message": row["_source"]["message"]
            },
        }
        yield doc

# Cuerpo de peticiones #######################################################
body =  {
        "size": 10000, # 10mil datos
        "sort": { "@timestamp": "asc"}, # Ordenar ascendente
        "query": {
          "bool": {
            "filter": [     
                {
                  "range": {
                    "@timestamp": {
                      "gte": "now-30d", # ¿La úlima hora? : "now-1h"
                       "lte": "now"
                    }
                  }
                },
              #   {
              #   "range": {
              #     "@timestamp": { #Columna que maneja el tiempo
              #       "lt": "now-15d" #Tiempo que se alojan los documentos en días "desde ahora hasta Xdías"
              #     }
              #   }
              # },
              {
                "term": {
                  "log.level": "ERROR" #Columna con el nombre de tipo de usuario CAMBIAR POR EL NOMBRE DE COLUMNA
                }
              }
            ]
          }
        }
      }


try:
    
    result = es.search(index="elastic*", body=body) # Resultado 
    helpers.bulk(es_main, generate_docs(result["hits"]["hits"])) # Helper para bulk
    resumen = es_main.count(index=index_name) # Rectificación

except Exception:
    logger.error(traceback.format_exc()) 