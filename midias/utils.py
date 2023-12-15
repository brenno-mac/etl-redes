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

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'googlecredentials.json'
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/USER/Desktop/ETL_Redes_Sociais/Instagram_Latam/Insights_Midia/story/googlecredentials.json"
project_id = 'datalake-2022'
client = bigquery.Client()
configinsta = "SELECT * FROM `datalake-2022.variaveis_de_ambiente.config_for_etl_instagram`"
configg = pd.read_gbq(configinsta, project_id = project_id, dialect = 'standard')

###DEFININDO TIMEZONES###

sptz = pytz.timezone("America/Sao_Paulo")

currenttime = datetime.now(sptz)

currenttimer = currenttime.strftime("%d/%m/%Y %H:%M")
currentdate = currenttime.strftime("%d/%m/%Y")

yesterday_obj = datetime.now() - timedelta(days = 1)
yesterday = yesterday_obj.strftime("%d/%m/%Y")

thirty_days_ago = datetime.now() - timedelta(days = 30)
date_start = time.mktime(thirty_days_ago.timetuple()) 
date_end = time.mktime(yesterday_obj.timetuple())



    ###IMPORTANDO AS CONFIGURAÇÕES###

base = configg['base'][0]
version = configg['version'][0]
ig_user_id_br = configg['ig_user_id_br'][0]
limite_postagens = configg['limite_postagens'][0]
access_token = configg['access_token'][0]





    ###FUNÇÕES###
    
def encher_tabela(tabela, coluna, indice):
    for i in tabela:    
        coluna.append(i['data'][indice]['values'][0]['value'])
        
def encher_id(tabela):
    for i in tabela:
        id.append(i['data'][0]['id']) 

def query(base, version, ig_user_id_br, limite_postagens, access_token):
    timeline = get(f"{base}/{version}/{ig_user_id_br}/media?fields=caption%2Ccomments_count%2Clike_count%2Cmedia_product_type%2Cmedia_type%2Cmedia_url%2Cpermalink%2Cthumbnail_url%2Ctimestamp&limit={limite_postagens}&access_token={access_token}")
    timeline = timeline.json()
    with open("instagram_media.json", "w") as file:
        file.write(json.dumps(timeline, indent = 4))
    timeline = open("instagram_media.json") 
    data = json.load(timeline)
    data = data['data']
    data_total = pd.DataFrame(data)        
    return data_total
     
     
            
ai = query(base, version, ig_user_id_br, limite_postagens, access_token)

def query_reels(base, version, ig_media_id, limite_postagens, access_token):
    data_reels_total = []
    reels = get(f"{base}/{version}/{ig_media_id}/insights?metric=comments%2Clikes%2Cplays%2Creach%2Csaved%2Cshares%2Ctotal_interactions&limit={limite_postagens}&access_token={access_token}")
    reels = reels.json()
    if "data" in reels:
        return reels        
    else:
        pass
    for x in ai[ai.media_product_type == 'REELS']['id']:
        tmp = query_reels(base, version, x, limite_postagens, access_token)
        if tmp is not None:
            data_reels_total.append(tmp)
        else:
            pass
    with open("data_reels_total.json", "w") as file:
        file.write(json.dumps(data_reels_total, indent = 4))
    dfs = []

    for entry in data_reels_total:
        data_list = entry.get('data', [])
        
        df = pd.DataFrame()
        
        
        for data in data_list:
            title = data.get('title')
            value = data.get('values', [{}])[0].get('value')
            entry_id = data.get('id')
            df[title] = [value]
            df['id'] = entry_id
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    for i in range(len(final_df['id'])):
        final_df['id'][i] = final_df['id'][i][:17]
        
    reels_full = pd.merge(ai,final_df,on='id',how='right')
    return reels_full




query_reels(base, version, ig_media_id_br, limite_postagens, access_token)