import os
from google.cloud import bigquery
import pandas_gbq
from requests import get
import pandas as pd
import json
from datetime import datetime, timedelta
import pytz        
import time


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecredentials.json"
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/USER/Desktop/ETL_Redes_Sociais/googlecredentials.json"
project_id = 'datalake-2022'
client = bigquery.Client()
configinsta = "SELECT * FROM `datalake-2022.variaveis_de_ambiente.config_for_etl_facebook`"
configg = pd.read_gbq(configinsta, project_id = project_id, dialect = 'standard')

###IMPORTANDO AS CONFIGURAÇÕES###

base = configg['base'][0]
facebook_user_id = configg['facebook_fis_id'][0]
access_token = configg['access_token_facebook_fis'][0]
limit = configg['limit'][0]



### DEFINIR QUERIES DE MÍDIA ###



def query_media(base, facebook_user_id, limit, access_token):
   media = get("{}/{}/feed?fields=id%2Cmessage%2Cstory%2Ccreated_time%2cattachments{{title,description, media,type, unshimmed_url}}&limit={}&access_token={}".format(base, facebook_user_id, limit, access_token))
   media = media.json()
   with open("ALOU.json", "w") as file:
       file.write(json.dumps(media, indent = 4))

def query_media_ind(base, media_id, access_token):
    media_ind = get("{}/{}/insights/post_engaged_users%2Cpost_negative_feedback_by_type%2Cpost_engaged_fan%2Cpost_clicks_by_type%2Cpost_impressions%2Cpost_impressions_viral%2Cpost_reactions_by_type_total?access_token={}".format(base, media_id, access_token))
    media_ind = media_ind.json()
    if "data" in media_ind:
        return media_ind        
    else:
        pass
    
def query_insights(base, facebook_user_id, access_token):
    insights = get("{}/{}/insights/page_engaged_users, page_total_actions, page_consumptions, page_consumptions_by_consumption_type, page_places_checkin_total, page_post_engagements, page_negative_feedback_by_type, page_positive_feedback_by_type, page_fans_online_per_day, page_fans_online, page_impressions, page_impressions_frequency_distribution, page_posts_impressions, page_actions_post_reactions_total, page_fans, page_fan_adds, page_fans_by_like_source, page_fans_by_unlike_source_unique, page_views_total?access_token={}".format(base, facebook_user_id, access_token))
    insights = insights.json()
    with open("AHA.json", "w") as file:
       file.write(json.dumps(insights, indent = 4))
    
    
def query_rel(base, facebook_user_id, access_token):
    rel = get("{}/{}/insights/page_fans_locale, page_fans_gender_age, page_fans_country?access_token={}".format(base, facebook_user_id, access_token))
    rel = rel.json()
    with open("REL.json", "w") as file:
       file.write(json.dumps(rel, indent = 4))
       
           
def encher_tabela(tabela, coluna, indice):
    for i in tabela:    
        coluna.append(i['data'][indice]['values'][0]['value'])
        
def encher_id(tabela):
    for i in tabela:
        id.append(i['data'][0]['id']) 

query_media(base, facebook_user_id, limit, access_token)
media = open("ALOU.json") 
media = json.load(media)
media = media['data']

media_total = pd.DataFrame(media) 

title = []
src = []
type = []
unshimmed_url = []


for i in media_total['attachments']:
    if (i['data'][0]['type'] == 'album'):
        title.append(i['data'][0]['title'])
    else:
        title.append(' ')

for i in media_total['attachments']:
    src.append(i['data'][0]['media']['image']['src'])
    
for i in media_total['attachments']:
    type.append(i['data'][0]['type'])
    
for i in media_total['attachments']:
    unshimmed_url.append(i['data'][0]['unshimmed_url'])
    
media_total['title'] = title
media_total['src'] = src
media_total['type'] = type
media_total['unshimmed_url'] = unshimmed_url

media_total = media_total.drop(columns=['attachments'])
data_total2 = []

for x in media_total['id']:
    tmp = query_media_ind(base, x, access_token)
    if tmp is not None:
        data_total2.append(tmp)
    else:
        pass

post_engaged_users, post_negative_feedback_by_type, post_engaged_fan, post_clicks_by_type, post_impressions, post_impressions_viral, post_reactions_by_type_total, id = [], [], [], [], [], [], [], []

encher_tabela(data_total2, post_engaged_users, 0)  
encher_tabela(data_total2, post_negative_feedback_by_type, 1)
encher_tabela(data_total2, post_engaged_fan, 2)
encher_tabela(data_total2, post_clicks_by_type, 3)
encher_tabela(data_total2, post_impressions, 4)
encher_tabela(data_total2, post_impressions_viral, 5)
encher_tabela(data_total2, post_reactions_by_type_total, 6)
encher_id(data_total2)

ala = {
    "post_engaged_users" : post_engaged_users,
    "post_negative_feedback_by_type": post_negative_feedback_by_type,
    "post_engaged_fan" : post_engaged_fan,
    "post_clicks_by_type": post_clicks_by_type,
    "post_impressions" : post_impressions,
    "post_impressions_viral": post_impressions_viral,
    "post_reactions_by_type_total" : post_reactions_by_type_total,
    "id" : id
    }         

ala = pd.DataFrame.from_dict(ala, orient='index').T

abc = ala['id'].str.split("/", expand=True)
ala = pd.concat([ala,abc[0]], axis=1 )
ala = ala.drop(columns=['id'])
ala = ala.rename(columns={0: "id"})

ala['post_clicks_by_type'] = ala['post_clicks_by_type'].astype(str).str.replace("[{}']", "", regex=True)
ala['post_reactions_by_type_total'] = ala['post_reactions_by_type_total'].astype(str).str.replace("[{}']", "", regex=True)
ala['post_negative_feedback_by_type'] = ala['post_negative_feedback_by_type'].astype(str).str.replace("[{}']", "", regex=True)

###NSIGHTS###

query_insights(base, facebook_user_id, access_token)

insights = open("AHA.json") 
insights = json.load(insights)
insights = insights['data']

insights_total = pd.DataFrame(insights) 
###DAILY###
insights_daily = insights_total[insights_total['period'] == 'day']
insights_daily['values'] = insights_daily['values'].astype(str).str.replace("[{}']", "", regex=True)

abcd = insights_daily['values'].str.split("0000,", expand=True)
abcd = abcd.drop(columns=[0])
abcdef = abcd[1].str.split(", end_time: ", expand=True)
abcdef[1] = abcdef[1].str[:10]
abcdef[0] = abcdef[0].astype(str).str.replace("value: ", "")
abcdef = abcdef.rename(columns={0: "value", 1:"end_time"})
insights_daily = pd.concat([insights_daily,abcdef], axis=1 )


insights_daily = insights_daily.drop(columns=['values', 'title', 'description'])
abcde = insights_daily['id'].str.split("/", expand=True)
insights_daily = pd.concat([insights_daily,abcde[0]], axis=1 )
insights_daily = insights_daily.drop(columns=['id'])
insights_daily = insights_daily.rename(columns={0: "id"})

###WEEKLY###

insights_week = insights_total[insights_total['period'] == 'week']
insights_week['values'] = insights_week['values'].astype(str).str.replace("[{}']", "", regex=True)

abcd = insights_week['values'].str.split("0000,", expand=True)
abcd = abcd.drop(columns=[0])
abcdef = abcd[1].str.split(", end_time: ", expand=True)
abcdef[1] = abcdef[1].str[:10]
abcdef[0] = abcdef[0].astype(str).str.replace("value: ", "")
abcdef = abcdef.rename(columns={0: "value", 1:"end_time"})
insights_week = pd.concat([insights_week,abcdef], axis=1 )

insights_week = insights_week.drop(columns=['values', 'title', 'description'])
abcde = insights_week['id'].str.split("/", expand=True)
insights_week = pd.concat([insights_week,abcde[0]], axis=1 )
insights_week = insights_week.drop(columns=['id'])
insights_week = insights_week.rename(columns={0: "id"})


###28d###

insights_28d = insights_total[insights_total['period'] == 'days_28']
insights_28d['values'] = insights_28d['values'].astype(str).str.replace("[{}']", "", regex=True)

abcd = insights_28d['values'].str.split("0000,", expand=True)
abcd = abcd.drop(columns=[0])
abcdef = abcd[1].str.split(", end_time: ", expand=True)
abcdef[1] = abcdef[1].str[:10]
abcdef[0] = abcdef[0].astype(str).str.replace("value: ", "")
abcdef = abcdef.rename(columns={0: "value", 1:"end_time"})
insights_28d = pd.concat([insights_28d,abcdef], axis=1 )

insights_28d = insights_28d.drop(columns=['values', 'title', 'description'])
abcde = insights_28d['id'].str.split("/", expand=True)
insights_28d = pd.concat([insights_28d,abcde[0]], axis=1 )
insights_28d = insights_28d.drop(columns=['id'])
insights_28d = insights_28d.rename(columns={0: "id"})


###INFORMAÇÕES AGREGADAS###

query_rel(base, facebook_user_id, access_token)
rel = open("REL.json") 
rel = json.load(rel)
rel = rel['data']
rel

rel = pd.DataFrame(rel) 
rel_local = rel['values'][0][0]['value']
rel_local = pd.DataFrame.from_dict(rel_local, orient='index')
rel_local.reset_index(inplace=True)
rel_local = rel_local.rename(columns={0: "value", 'index':"local"})

l = [rel['name'][0]] * len(rel_local)
l = pd.DataFrame(l)
rel_local = pd.concat([rel_local,l], axis=1 )
rel_local = rel_local.rename(columns={0: "type"})
abcdefg = rel['id'].str.split("/", expand=True)
l = [abcdefg[0][0]] * len(rel_local)
l = pd.DataFrame(l)
rel_local = pd.concat([rel_local,l], axis=1 )
rel_local = rel_local.rename(columns={0: "id"})
rel_local = rel_local.sort_values(['value'],ascending = [False])


rel_genderage = rel['values'][1][0]['value']
rel_genderage = pd.DataFrame.from_dict(rel_genderage, orient='index')
rel_genderage.reset_index(inplace=True)
rel_genderage = rel_genderage.rename(columns={0: "value", 'index':"local"})

l = [rel['name'][1]] * len(rel_genderage)
l = pd.DataFrame(l)
rel_genderage = pd.concat([rel_genderage,l], axis=1 )
rel_genderage = rel_genderage.rename(columns={0: "type"})
abcdefg = rel['id'].str.split("/", expand=True)
l = [abcdefg[0][0]] * len(rel_genderage)
l = pd.DataFrame(l)
rel_genderage = pd.concat([rel_genderage,l], axis=1 )
rel_genderage = rel_genderage.rename(columns={0: "id"})
rel_genderage = rel_genderage.sort_values(['value'],ascending = [False])

rel_country = rel['values'][2][0]['value']
rel_country = pd.DataFrame.from_dict(rel_country, orient='index')
rel_country.reset_index(inplace=True)
rel_country = rel_country.rename(columns={0: "value", 'index':"local"})

l = [rel['name'][2]] * len(rel_country)
l = pd.DataFrame(l)
rel_country = pd.concat([rel_country,l], axis=1 )
rel_country = rel_country.rename(columns={0: "type"})
abcdefg = rel['id'].str.split("/", expand=True)
l = [abcdefg[0][0]] * len(rel_country)
l = pd.DataFrame(l)
rel_country = pd.concat([rel_country,l], axis=1 )
rel_country = rel_country.rename(columns={0: "id"})
rel_country = rel_country.sort_values(['value'],ascending = [False])

rel_total = pd.concat([rel_local, rel_genderage, rel_country])


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

rel_total["date_etl"] = currentdate
ala["date_etl"] = currentdate
insights_daily["date_etl"] = currentdate
insights_week["date_etl"] = currentdate
insights_28d["date_etl"] = currentdate
media_total['date_etl'] = currentdate


#%%

###COLOCANDO VARIÁVEIS NO BIGQUERY###

job = client.load_table_from_dataframe(media_total,'facebookbr.midia_agreg')
job = client.load_table_from_dataframe(rel_total,'facebookbr.dados_agreg')
job = client.load_table_from_dataframe(ala,'facebookbr.midia')
job = client.load_table_from_dataframe(insights_daily,'facebookbr.insightsdia')
job = client.load_table_from_dataframe(insights_week,'facebookbr.insightssemana')
job = client.load_table_from_dataframe(insights_28d,'facebookbr.28d')