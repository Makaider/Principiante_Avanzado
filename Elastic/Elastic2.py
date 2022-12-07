# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 15:28:17 2022

@author: Jaider R.
"""

from elasticsearch import Elasticsearch, helpers
import logging
import traceback


#nombre del indice ##########################################################
index_name = "logs-elastic" #nombre de indice

# logging ####################################################################
logger = logging.getlogger("log-elastic")
logger.setlevel(logging.debug)
fh =logging.filehandler("logs-elastic.log")
fh.setlevel(logging.debug)
logger.addhandler(fh)
formatter = logging.formatter("[%(asctime)s] [%(levelname)s] %(message)s")
fh.setformatter(formatter)

# enviar data ("bulkear") ###################################################


body= {
  "query": {
    "match": {
      "log.level": "INFO"
    }
  }
}


index_name = "logs-elastic" #nombre de indice