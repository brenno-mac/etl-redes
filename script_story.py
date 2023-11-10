#%%

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
ig_user_id = configg['ig_user_id_seliga'][0]
limite_postagens = configg['limite_postagens'][0]
access_token = configg['access_token'][0]

#%%


### DEFINIR FUNÇÃO DE QUERY INICIAL###

def query(base, version, ig_user_id, limite_postagens, access_token):
    timeline = get("{}/{}/{}/media?fields=caption%2Ccomments_count%2Clike_count%2Cmedia_product_type%2Cmedia_type%2Cmedia_url%2Cpermalink%2Cthumbnail_url%2Ctimestamp&limit={}&access_token={}".format(base, version, ig_user_id, limite_postagens, access_token))
    timeline = timeline.json()
    with open("instagram_media.json", "w") as file:
        file.write(json.dumps(timeline, indent = 4))

def encher_tabela(tabela, coluna, indice):
    for i in tabela:    
        coluna.append(i['data'][indice]['values'][0]['value'])
        
def encher_id(tabela):
    for i in tabela:
        id.append(i['data'][0]['id']) 

def query_story(base, version, ig_media_id, access_token):   
    story = get("{}/{}/{}/insights?metric=exits%2Cimpressions%2Creach%2Creplies%2Ctaps_forward%2Ctaps_back&access_token={}".format(base, version, ig_media_id, access_token))
    story = story.json()
    if "data" in story:
        return story        
    else:
        pass        
        
###PEGANDO OS ID'S DOS STORIES DIÁRIOS###

stories_diarios = get("{}/{}/{}/stories?media_product_type=STORY&access_token={}".format(base, version, ig_user_id, access_token))
stories_diarios = stories_diarios.json()   
story_id = []
for i in stories_diarios['data']:
    story_id.append(i['id'])


print('fase 1 ok')
#%%
data_story_total = []

for i in story_id:
    tmp5 = query_story(base, version, i, access_token)
    if tmp5 is not None:
        data_story_total.append(tmp5)
    else:
        pass

with open("data_story_total.json", "w") as file:
    file.write(json.dumps(data_story_total, indent = 4))   
    
#%%

###STORY###
exits,impressions,reach,replies,taps_forward,taps_back,id,carousel = [],[],[],[],[],[],[],[]

encher_tabela(data_story_total, exits, 0)  
encher_tabela(data_story_total, impressions, 1)
encher_tabela(data_story_total, reach, 2)
encher_tabela(data_story_total, replies, 3)
encher_tabela(data_story_total, taps_forward, 4)
encher_tabela(data_story_total, taps_back, 5)
encher_id(data_story_total)


story = {
    "exits" : exits,
    "impressions": impressions,
    "reach" : reach,
    "replies" : replies,
    "taps_forward": taps_forward,
    "taps_back" : taps_back,
    "id" :  id
    }         

story = pd.DataFrame.from_dict(story, orient='index').T

for i in range(len(story['id'])):
    story['id'][i] = story['id'][i][:17]


print('fase 3 ok')

story["date_etl"] = currenttimer

#%%

###COLOCANDO VARIÁVEIS NO BIGQUERY###

job = client.load_table_from_dataframe(story,'instagramseliga.story')

print('fase 7 ok')