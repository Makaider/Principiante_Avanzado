from elasticsearch import Elasticsearch, helpers
import logging
import traceback

# Credenciales Clúster nuevo #################################################
ELASTIC_ID = "deployment_tests_real_experience:ZWFzdHVzMi5henVyZS5lbGFzdGljLWNsb3VkLmNvbTo0NDMkMzRiN2I5MGM5YWJjNDQ1Nzg0N2ViM2ZmNWZkNmI3N2UkOTVhZDVhYmU5NzY3NGVjNGE1ZGQ0MWZjNWFlOGZjMmQ="
KEY_ID = "THoKq4EBRhQifaF5KZIE"
MASTER = "qwVtxF9nQMWM-Lekitcsfw"


#Activacion cliente elasticsearch ############################################
es = Elasticsearch(cloud_id=ELASTIC_ID, api_key=(KEY_ID, MASTER), timeout=30) #Requerimiento

# Cuerpo de peticiones #######################################################
body =      {
      "query": {
        "match": {
          "log.level": "INFO"
        }
      }
    }

result = es.search(index="elastic-cloud-logs-7-2022.07.21-000002", body=body) #Resultado