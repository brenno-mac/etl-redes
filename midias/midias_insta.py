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
from utils import data_total, query2, query_reels, query_carousel, query_image, query_video_tv, query_video_p

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'googlecredentials.json'
project_id = 'datalake-2022'
client = bigquery.Client()
configinsta = "SELECT * FROM `datalake-2022.variaveis_de_ambiente.config_for_etl_instagram`"
configg = pd.read_gbq(configinsta, project_id = project_id, dialect = 'standard')



sptz = pytz.timezone("America/Sao_Paulo")

currenttime = datetime.now(sptz)

currentdate = currenttime.strftime("%Y-%m-%d")

    ###IMPORTANDO AS CONFIGURAÇÕES###

base = configg['base'][0]
version = configg['version'][0]
ig_user_id_br = configg['ig_user_id_br'][0]
ig_user_id_latam = configg['ig_user_id_latam'][0]
ig_user_id_seliga = configg['ig_user_id_seliga'][0]
limite_postagens = configg['limite_postagens'][0]
access_token = configg['access_token'][0]


###INSTABR###
data_total(base, version, ig_user_id_br, limite_postagens, access_token, 'instagrambr')
df_total_br = query2(base, version, ig_user_id_br, limite_postagens, access_token)
query_reels(df_total_br, base, version, limite_postagens, access_token, 'instagrambr')
query_carousel(df_total_br ,base, version, limite_postagens, access_token, 'instagrambr')
query_image(df_total_br, base, version, limite_postagens, access_token, 'instagrambr')
query_video_tv(df_total_br, base, version, limite_postagens, access_token, 'instagrambr')
query_video_p(df_total_br, base, version, limite_postagens, access_token, 'instagrambr')

###INSTALATAM###
# data_total(base, version, ig_user_id_latam, limite_postagens, access_token, 'instagrambr')
# df_total_latam = query2(base, version, ig_user_id_latam, limite_postagens, access_token)
# query_reels(df_total_latam, base, version, limite_postagens, access_token, 'instagrambr')
# query_carousel(df_total_latam ,base, version, limite_postagens, access_token, 'instagrambr')
# query_image(df_total_latam, base, version, limite_postagens, access_token, 'instagrambr')
# query_video_tv(df_total_latam, base, version, limite_postagens, access_token, 'instagrambr')
# query_video_p(df_total_latam, base, version, limite_postagens, access_token, 'instagrambr')

###INSTASELIGA###
# data_total(base, version, ig_user_id_seliga, limite_postagens, access_token, 'instagrambr')
# df_total_seliga = query2(base, version, ig_user_id_latam, limite_postagens, access_token)
# query_reels(df_total_seliga, base, version, limite_postagens, access_token, 'instagrambr')
# query_carousel(df_total_seliga ,base, version, limite_postagens, access_token, 'instagrambr')
# query_image(df_total_seliga, base, version, limite_postagens, access_token, 'instagrambr')
# query_video_tv(df_total_seliga, base, version, limite_postagens, access_token, 'instagrambr')
# query_video_p(df_total_seliga, base, version, limite_postagens, access_token, 'instagrambr')
