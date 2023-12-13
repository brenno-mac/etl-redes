###IMPORTANDO PACOTES NECESSÁRIOS###

import pandas as pd
import tomli
from datetime import datetime, timedelta
import pytz        
import time
from requests import get
import json
import os
from google.cloud import bigquery
import pandas_gbq
from utils import query_demographics, query_follow_total, query_dailymetrics

    ###TEMPO###

sptz = pytz.timezone("America/Sao_Paulo")
currenttime = datetime.now(sptz)
currentdate = currenttime.strftime("%Y-%m-%d")
yesterday_obj = datetime.now() - timedelta(days = 1)
yesterday = yesterday_obj.strftime("%d/%m/%Y")
thirty_days_ago = datetime.now() - timedelta(days = 30)
date_start = time.mktime(thirty_days_ago.timetuple()) 
date_end = time.mktime(yesterday_obj.timetuple())

    ###CRIANDO O CLIENTE###

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'googlecredentials.json'
project_id = 'datalake-2022'
client = bigquery.Client()
configinsta = "SELECT * FROM `datalake-2022.variaveis_de_ambiente.config_for_etl_instagram`"
configg = pd.read_gbq(configinsta, project_id = project_id, dialect = 'standard')

    ###IMPORTANDO AS CONFIGURAÇÕES###

base = configg['base'][0]
version = configg['version'][0]
ig_user_id_br = configg['ig_user_id_br'][0]
limite_postagens = configg['limite_postagens'][0]
access_token = configg['access_token'][0]
ig_user_id_latam = configg['ig_user_id_latam'][0]
ig_user_id_seliga = configg['ig_user_id_seliga'][0]


    ###RODA###
    

query_demographics(base, version, ig_user_id_br, access_token, 'instagrambr')
# query_demographics(base, version, ig_user_id_latam, access_token, 'instagramlatam')
query_demographics(base, version, ig_user_id_seliga, access_token, 'instagramseliga')

query_dailymetrics(base, version, ig_user_id_br, access_token, 'instagrambr')
# query_dailymetrics(base, version, ig_user_id_latam, access_token, 'instagramlatam')
query_dailymetrics(base, version, ig_user_id_seliga, access_token, 'instagramseliga')

query_follow_total(base, version, ig_user_id_br,access_token, 'instagrambr')
# query_follow_total(base, version, ig_user_id_latam,access_token, 'instagramlatam')
query_follow_total(base, version, ig_user_id_seliga,access_token, 'instagramseliga')