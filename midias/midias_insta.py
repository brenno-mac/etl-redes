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
ig_user_id = configg['ig_user_id_br'][0]
limite_postagens = configg['limite_postagens'][0]
access_token = configg['access_token'][0]

### DEFINIR FUNÇÃO DE QUERY INICIAL###

def query(base, version, ig_user_id, limite_postagens, access_token):
    timeline = get("{}/{}/{}/media?fields=caption%2Ccomments_count%2Clike_count%2Cmedia_product_type%2Cmedia_type%2Cmedia_url%2Cpermalink%2Cthumbnail_url%2Ctimestamp&limit={}&access_token={}".format(base, version, ig_user_id, limite_postagens, access_token))
    timeline = timeline.json()
    with open("instagram_media.json", "w") as file:
        file.write(json.dumps(timeline, indent = 4))
        
query(base, version, ig_user_id, limite_postagens, access_token)
timeline = open("instagram_media.json") 
data = json.load(timeline)
data = data['data']

data_total = pd.DataFrame(data)        

print('fase 1 ok')

#%%
    
### DEFINIR QUERIES DE MÍDIA ###

def query_reels(base, version, ig_media_id, limite_postagens, access_token):
    reels = get("{}/{}/{}/insights?metric=comments%2Clikes%2Cplays%2Creach%2Csaved%2Cshares%2Ctotal_interactions&limit={}&access_token={}".format(base, version, ig_media_id, limite_postagens, access_token))
    reels = reels.json()
    if "data" in reels:
        return reels        
    else:
        pass
    
def query_carousel(base, version, ig_media_id, limite_postagens, access_token):    
    carousel = get("{}/{}/{}/insights?metric=carousel_album_engagement%2Ccarousel_album_impressions%2Ccarousel_album_reach%2Ccarousel_album_saved%2Ccarousel_album_video_views&limit={}&access_token={}".format(base, version, ig_media_id, limite_postagens, access_token))
    carousel = carousel.json()
    if "data" in carousel:
        return carousel        
    else:
        pass    
    
def query_image(base, version, ig_media_id, limite_postagens, access_token):
    image = get("{}/{}/{}/insights?metric=engagement%2Cimpressions%2Creach%2Csaved&limit={}&access_token={}".format(base, version, ig_media_id, limite_postagens, access_token))
    image = image.json()
    if "data" in image:
        return image       
    else:
        pass
     
def query_video(base, version, ig_media_id, limite_postagens, access_token):
    video = get("{}/{}/{}/insights?metric=engagement%2Cimpressions%2Creach%2Csaved%2Cvideo_views&limit={}&access_token={}".format(base, version, ig_media_id, limite_postagens, access_token))
    video = video.json()
    if "data" in video:
        return video      
    else:
        pass

#%%

###POPULANDO A BASE DE DADOS###

data_reels_total = []
data_carousel_total = []
data_image_total = []
data_video_total = []
data_story_total = [] 
 
def encher_tabela(tabela, coluna, indice):
    for i in tabela:    
        coluna.append(i['data'][indice]['values'][0]['value'])
        
def encher_id(tabela):
    for i in tabela:
        id.append(i['data'][0]['id'])  

for x in data_total[data_total.media_product_type == 'REELS']['id']:
    tmp = query_reels(base, version, x, limite_postagens, access_token)
    if tmp is not None:
        data_reels_total.append(tmp)
    else:
        pass

for x in data_total[(data_total.media_product_type == 'FEED') & (data_total.media_type == 'CAROUSEL_ALBUM')]['id']:
    tmp2 = query_carousel(base, version, x, limite_postagens, access_token)
    if tmp2 is not None:
        data_carousel_total.append(tmp2)
    else:
        pass
    
for x in data_total[(data_total.media_type == 'IMAGE') & (data_total.media_product_type == 'FEED')]['id']:
    tmp3 = query_image(base, version, x, limite_postagens, access_token)
    if tmp3 is not None:
        data_image_total.append(tmp3)
    else:
        pass

for x in data_total[(data_total.media_product_type == 'FEED') & (data_total.media_type == 'VIDEO')]['id']:
    tmp4 = query_video(base, version, x, limite_postagens, access_token)
    if tmp4 is not None:
        data_video_total.append(tmp4)
    else:
        pass

with open("data_reels_total.json", "w") as file:
    file.write(json.dumps(data_reels_total, indent = 4))


with open("data_carousel_total.json", "w") as file:
    file.write(json.dumps(data_carousel_total, indent = 4))

with open("data_image_total.json", "w") as file:
    file.write(json.dumps(data_image_total, indent = 4))

with open("data_video_total.json", "w") as file:
    file.write(json.dumps(data_video_total, indent = 4))

print('fase 2 ok')    

#%%    

###REELS###

comments, likes, plays, reach, saved, shares, total_interactions, id, reels = [],[],[],[],[],[],[],[],[]      

encher_tabela(data_reels_total, comments, 0)  
encher_tabela(data_reels_total, likes, 1)
encher_tabela(data_reels_total, plays, 2)
encher_tabela(data_reels_total, reach, 3)
encher_tabela(data_reels_total, saved, 4)
encher_tabela(data_reels_total, shares, 5)
encher_tabela(data_reels_total, total_interactions, 6)
encher_id(data_reels_total)

reels = {
    "comments" : comments,
    "likes": likes,
    "plays" : plays,
    "reach": reach,
    "saved" : saved,
    "shares": shares,
    "total_interactions" : total_interactions,
    "id" :  id
    }         

reels = pd.DataFrame.from_dict(reels, orient='index').T

for i in range(len(reels['id'])):
    reels['id'][i] = reels['id'][i][:17]

    
reels_full = pd.merge(data_total,reels,on='id',how='right')

#%%

###CAROUSEL###

carousel_album_engagement,carousel_album_impressions,carousel_album_reach,saved,video_views,id,carousel = [],[],[],[],[],[],[]

encher_tabela(data_carousel_total, carousel_album_engagement, 0)  
encher_tabela(data_carousel_total, carousel_album_impressions, 1)
encher_tabela(data_carousel_total, carousel_album_reach, 2)
encher_tabela(data_carousel_total, saved, 3)
encher_tabela(data_carousel_total, video_views, 4)
encher_id(data_carousel_total)


carousel = {
    "carousel_album_engagement" : carousel_album_engagement,
    "carousel_album_impressions": carousel_album_impressions,
    "carousel_album_reach" : carousel_album_reach,
    "saved" : saved,
    "video_views": video_views,
    "id" :  id
    }         

carousel = pd.DataFrame.from_dict(carousel, orient='index').T

for i in range(len(carousel['id'])):
    carousel['id'][i] = carousel['id'][i][:17]

    
carousel_full = pd.merge(data_total,carousel,on='id',how='right')

#%%

###IMAGE###

engagement, impressions, reach, saved, id, image = [],[],[],[],[],[]

encher_tabela(data_image_total, engagement, 0)  
encher_tabela(data_image_total, impressions, 1)
encher_tabela(data_image_total, reach, 2)
encher_tabela(data_image_total, saved, 3)
encher_id(data_image_total)

image = {
    "engagement" : engagement,
    "impressions": impressions,
    "reach" : reach,
    "saved" : saved,
    "id" :  id
    }         

image = pd.DataFrame.from_dict(image, orient='index').T

for i in range(len(image['id'])):
    image['id'][i] = image['id'][i][:17]

    
image_full = pd.merge(data_total,image,on='id',how='right')

#%%

###VIDEO###

engagement, impressions, reach, saved, video_views, id, video = [],[],[],[],[],[],[]

encher_tabela(data_video_total, engagement, 0)  
encher_tabela(data_video_total, impressions, 1)
encher_tabela(data_video_total, reach, 2)
encher_tabela(data_video_total, saved, 3)
encher_tabela(data_video_total, video_views, 4)
encher_id(data_video_total)

video = {
        "engagement" : engagement,
        "impressions" : impressions,
        "reach" : reach,
        "saved" : saved,
        "video_views" : video_views,
        "id" :  id
        }         

video = pd.DataFrame.from_dict(video, orient='index').T

for i in range(len(video['id'])):
    video['id'][i] = video['id'][i][:17]

    
video_full = pd.merge(data_total,video,on='id',how='right')

#%%

###IDENTIFICANDO O TEMPO###

data_total["date_etl"] = currenttimer
reels_full["date_etl"] = currenttimer
carousel_full["date_etl"] = currenttimer
image_full["date_etl"] = currenttimer
video_full["date_etl"] = currenttimer

#%%

###COLOCANDO VARIÁVEIS NO BIGQUERY###

job = client.load_table_from_dataframe(data_total,'instagramseliga.data_total')
job = client.load_table_from_dataframe(reels_full,'instagramseliga.reels')
job = client.load_table_from_dataframe(carousel_full,'instagramseliga.carousel')
job = client.load_table_from_dataframe(image_full,'instagramseliga.image')
if video_full.empty == False:
    job = client.load_table_from_dataframe(video_full,'instagramseliga.video')
else: 
    pass
print('fase 7 ok')


# %%
