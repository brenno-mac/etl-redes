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

### DEFINIR QUERIES DE MÍDIA ###

def query_rel(base, version, ig_user_id, access_token):
   rel = get("{}/{}/{}/insights?metric=audience_locale%2Caudience_country%2Caudience_city%2Caudience_gender_age&period=lifetime&access_token={}".format(base, version, ig_user_id, access_token))
   rel = rel.json()
   with open("instagram_rel.json", "w") as file:
       file.write(json.dumps(rel, indent = 4))
       
def query_rel2(base, version, ig_user_id, access_token):
    rel2 = get("{}/{}/{}/insights?metric=reach%2Cprofile_views%2Cfollower_count&period=day&access_token={}".format(base, version, ig_user_id, access_token))
    rel2 = rel2.json()
    with open("instagram_rel2.json", "w") as file:
        file.write(json.dumps(rel2, indent = 4))
        
def query_follow(base, version, ig_user_id,date_start, date_end, access_token):                       
    follow = get("{}/{}/{}/insights?metric=follower_count&since={}&until={}&period=day&access_token={}".format(base, version, ig_user_id, date_start, date_end, access_token))                          
    follow = follow.json()
    with open("follow.json", "w") as file:
        file.write(json.dumps(follow, indent = 4))
        
def query_follow_total(base, version, ig_user_id,access_token):
    follow_total = get("{}/{}/{}?fields=followers_count&access_token={}".format(base, version, ig_user_id, access_token))
    follow_total = follow_total.json()
    with open("follow_total.json", "w") as file:
        file.write(json.dumps(follow_total, indent = 4))

def encher_tabela(tabela, coluna, indice):
    for i in tabela:    
        coluna.append(i['data'][indice]['values'][0]['value'])
        
def encher_id(tabela):
    for i in tabela:
        id.append(i['data'][0]['id'])  

aaa = []
bbb = []
###RELATÓRIO###

query_rel(base, version, ig_user_id,access_token)
rel = open("instagram_rel.json") 
rel = json.load(rel)
rel = rel['data']

query_rel2(base, version, ig_user_id,access_token)
rel2 = open("instagram_rel2.json") 
rel2 = json.load(rel2)
rel2 = rel2['data']

query_follow(base, version, ig_user_id, date_start, date_end, access_token)
follow = open("follow.json")
follow = json.load(follow)
follow = follow['data']

query_follow_total(base, version, ig_user_id, access_token)
follow_total = open("follow_total.json")
follow_total = json.load(follow_total)
follow_total = pd.DataFrame.from_dict(follow_total, orient='index').T
follow_total = follow_total['followers_count']
follow_total = pd.DataFrame(follow_total)

for i in follow[0]['values']:    
    aaa.append(i['value'])
    
for i in follow[0]['values']:    
   bbb.append(i['end_time'])    
   
follow = {'followers' : aaa,
          'date' : bbb
    }   
    
follow = pd.DataFrame(follow)
follow['date'] = follow['date'].str[:10]

rel_full = pd.DataFrame(rel)
relday = pd.DataFrame(rel2)
relday['values'][0] = relday['values'][0][0]['value']
relday['values'][1] = relday['values'][1][0]['value']
relday['values'][2] = relday['values'][2][0]['value']
relday = pd.DataFrame(relday).T

relday.drop(['period', 'title', 'description', 'id', 'name'], inplace = True)    
relday.rename({0: 'reach', 1: 'profile_views', 2: 'follower_count'}, axis=1, inplace=True)  



audience_city = pd.DataFrame.from_dict(rel_full['values'][2][0]['value'], orient='index')
audience_city['type'] = 'city'
audience_city['object'] = audience_city.index
audience_city.rename({0:'value'}, axis = 1, inplace = True)

audience_local = pd.DataFrame.from_dict(rel_full['values'][0][0]['value'], orient='index')
audience_local['type'] = 'local'
audience_local['object'] = audience_local.index
audience_local.rename({0:'value'}, axis = 1, inplace = True)

audience_country = pd.DataFrame.from_dict(rel_full['values'][1][0]['value'], orient='index')
audience_country['type'] = 'country'
audience_country['object'] = audience_country.index
audience_country.rename({0:'value'}, axis = 1, inplace = True)

audience_gender_age = pd.DataFrame.from_dict(rel_full['values'][3][0]['value'], orient='index')
audience_gender_age['type'] = 'gender_age'
audience_gender_age['object'] = audience_gender_age.index
audience_gender_age.rename({0:'value'}, axis = 1, inplace = True)

print('fase 4 ok')

###IDENTIFICANDO O TEMPO###

audience_local['date_etl'] = currentdate
audience_country['date_etl'] = currentdate
audience_city['date_etl'] = currentdate
audience_gender_age['date_etl'] = currentdate
relday['date_etl'] = yesterday
follow['date_etl'] = currentdate
follow_total['date_etl'] = currentdate


###COLOCANDO VARIÁVEIS NO BIGQUERY###

client = bigquery.Client()

job = client.load_table_from_dataframe(audience_city,'instagramseliga.relatorio2')
job = client.load_table_from_dataframe(audience_local,'instagramseliga.relatorio2')
job = client.load_table_from_dataframe(audience_country,'instagramseliga.relatorio2')
job = client.load_table_from_dataframe(audience_gender_age,'instagramseliga.relatorio2')
job = client.load_table_from_dataframe(follow_total,'instagramseliga.dailyfollowers')
job = client.load_table_from_dataframe(follow,'instagramseliga.followers_ganhos')
job = client.load_table_from_dataframe(relday,'instagramseliga.dailymetrics')

print('fase 7 ok')