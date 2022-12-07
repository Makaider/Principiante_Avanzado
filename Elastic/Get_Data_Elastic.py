# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 07:49:32 2022

@author: Jaider R.
"""

## que sera ese 15s¡mlmlce

## vd mv fjnvk dl

###### librerias ##############################################################

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd

###### Credenciales CLúster principal #########################################

ELASTIC_ID_Main = "deployment_real_experience_001:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo5MjQzJGFlMzM3YWI2MmNlMTQ0YmFhMDIyMDFlNTUyZTNhNGMzJGEwNDk3MmY3Mzg4NjQzOWI4ZGVjMDZhYzFhMmU1YTA1"
KEY_ID_Main = "EEtHnHoBOnRVzkpI3oRI"
MASTER_Main = "6rwJg4lUQN2OXuFCzNDMMQ"

####### Credenciales Clúster nuevo ############################################

ELASTIC_ID = "deployment_tests_real_experience:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo0NDMkMzRiN2I5MGM5YWJjNDQ1Nzg0N2ViM2ZmNWZkNmI3N2UkOTVhZDVhYmU5NzY3NGVjNGE1ZGQ0MWZjNWFlOGZjMmQ="
KEY_ID = "THoKq4EBRhQifaF5KZIE"
MASTER = "qwVtxF9nQMWM-Lekitcsfw"

####### Activación clientes Elasticsearch #####################################

es = Elasticsearch(cloud_id=ELASTIC_ID, api_key=(KEY_ID, MASTER), timeout=30) # Requerimiento
es_main = Elasticsearch(cloud_id=ELASTIC_ID_Main, api_key=(KEY_ID_Main, MASTER_Main), timeout=30) # Principal

####### Bajar la data requerida ###############################################

def get_data_from_elastic():
    
####### Consulta de busqueda a Elastic ########################################
    query = {
        "query": {
            "match": {
                "agent.name": "probe.aaaab.real.net.co"
            }
        }
    }
####### Escaneo de funciones para obtener la Data #############################

    rel = scan(client=es,             
               query=query,                                     
         #    scroll='1m',
               index='speedtest*')
              # raise_on_error=True,
              # preserve_order=False,
              # clear_scroll=True)
    
####### Mantener una respuesta en la lista ###################################
    
    result = list(rel)
    temp = []
    
# Solo necesitamos '_source', que tiene todos los campos requeridos.
# Esto elimina los metadatos de búsqueda elástica como _id, type, _index.
    
    for hit in result:
        temp.append(hit['_source'])
        
    # Create a dataframe.
    df = pd.DataFrame(temp)
    return df

df = get_data_from_elastic()
print(df.head())