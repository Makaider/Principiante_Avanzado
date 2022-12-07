# -*- coding: utf-8 -*-
"""
Created on Mon Jun 27 21:40:01 2022

@author: Daniel
"""


# LIBRERÍAS ##################################################################
import pandas as pd
import traceback
import logging
import re
from datetime import timedelta


# CONSTANTES #################################################################

FILENAME = "/usr/local/real_eyes_probe_scripts/pfsense.err.log"
LOG_FILENAME = '/usr/local/real_eyes_probe_scripts/pfsense_logs_reader/logs-pfsense.log'
COLS = ["Date", "Message"]



# LOGGER #####################################################################
logger = logging.getLogger('PFSENSE')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOG_FILENAME)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(name)s]: %(message)s')
fh.setFormatter(formatter)

# CÓDIGO ##################################################################
try:
    data = pd.read_csv(FILENAME, names=COLS, sep="-;-", engine='Python')
    print(data["Date"].max(), "Primero")
    data['Date'] = pd.to_datetime(data['Date'], format="%d-%m-%y-%H-%M-%S") #Convertir la columna de tiempo en formato "DATE"
    print(data["Date"].max(), "Segundo")    
    print(data["Date"].max(), "Tercero")
    # data['Date'] = data['Date'].astype('datetime64[s]') #Convertir a segundos
    
    if pd.to_timedelta("2.5m") >> pd.Timestamp.now() - data["Date"].max(): #EL PRIMER IF VA AQUÍ. Debe ser >
    # print(re.search("Error: (.+)", data["Message"].max()))
    # logger.error(list(data[data['Date'] == data["Date"].max()].iloc[0]))
        logger.error(re.search("Error(.+)", data.at[data.index.max(), "Message"]))
        print("Llegó c:")
        # data.at[data.index.max(),"lon"]

except Exception:
    logger.error(traceback.format_exc())
    
except data:
    pd.read_csv(FILENAME, names=COLS, sep="-;-")    
    