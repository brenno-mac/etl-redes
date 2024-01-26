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
project_id = 'datalake-2022'
client = bigquery.Client()
configinsta = "SELECT * FROM `datalake-2022.variaveis_de_ambiente.config_for_etl_instagram`"
configg = pd.read_gbq(configinsta, project_id = project_id, dialect = 'standard')

###DEFININDO TIMEZONES###

sptz = pytz.timezone("America/Sao_Paulo")

currenttime = datetime.now(sptz)

currentdate = currenttime.strftime("%Y-%m-%d")

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
    
def data_total(base, version, ig_user_id, limite_postagens, access_token, database):
    timeline = get(f"{base}/{version}/{ig_user_id}/media?fields=caption%2Ccomments_count%2Clike_count%2Cmedia_product_type%2Cmedia_type%2Cmedia_url%2Cpermalink%2Cthumbnail_url%2Ctimestamp&limit={limite_postagens}&access_token={access_token}")
    timeline = timeline.json()
    with open("instagram_media.json", "w") as file:
        file.write(json.dumps(timeline, indent = 4))
    timeline = open("instagram_media.json") 
    data = json.load(timeline)
    data = data['data']
    df = pd.DataFrame(data) 
    
    df.drop(columns=['thumbnail_url'], inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%dT%H:%M:%S%z", errors='coerce')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df['date_etl'] = currentdate
    df['date_etl'] = pd.to_datetime(df['date_etl'], format="%Y-%m-%d")
    
    schema_data = [
        bigquery.SchemaField('caption','STRING'),
        bigquery.SchemaField('comments_count','INT64'),
        bigquery.SchemaField('like_count','INT64'),
        bigquery.SchemaField('media_product_type','STRING'),
        bigquery.SchemaField('media_type','STRING'),
        bigquery.SchemaField('media_url','STRING'),
        bigquery.SchemaField('permalink','STRING'),
        bigquery.SchemaField('timestamp','DATETIME'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('date_etl','DATE'),
    ]

    job_config_data = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_data)

    foi = client.load_table_from_dataframe(df,f'{database}.data_total', job_config=job_config_data)
          
    return foi.result
     
def query2(base, version, ig_user_id, limite_postagens, access_token):
    timeline = get(f"{base}/{version}/{ig_user_id}/media?fields=caption%2Ccomments_count%2Clike_count%2Cmedia_product_type%2Cmedia_type%2Cmedia_url%2Cpermalink%2Cthumbnail_url%2Ctimestamp&limit={limite_postagens}&access_token={access_token}")
    timeline = timeline.json()
    with open("instagram_media.json", "w") as file:
        file.write(json.dumps(timeline, indent = 4))
    timeline = open("instagram_media.json") 
    data = json.load(timeline)
    data = data['data']
    df = pd.DataFrame(data) 
    
    df.drop(columns=['thumbnail_url'], inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%dT%H:%M:%S%z", errors='coerce')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df['date_etl'] = currentdate
    # df['date_etl'] = pd.to_datetime(df['date_etl'], format="%d/%m/%Y", errors='coerce')
    
    return df
                 
# ai = query2(base, version, 17841452302292722, limite_postagens, access_token)

def query_reels(df,base, version, limite_postagens, access_token, database):
    data_reels_total = []
    for x in df[df.media_product_type == 'REELS']['id']:
        tmp = get(f"{base}/{version}/{x}/insights?metric=comments%2Clikes%2Cplays%2Creach%2Csaved%2Cshares%2Ctotal_interactions&limit={limite_postagens}&access_token={access_token}")
        tmp = tmp.json()
        if tmp is not None:
            data_reels_total.append(tmp)
        else:
            pass
    dfs = []

    for entry in data_reels_total:
        data_list = entry.get('data', [])
        
        df_tmp = pd.DataFrame()
        
        for data in data_list:
            title = data.get('title')
            value = data.get('values', [{}])[0].get('value')
            entry_id = data.get('id')
            df_tmp[title] = [value]
            df_tmp['id'] = entry_id
        dfs.append(df_tmp)

    final_df = pd.concat(dfs, ignore_index=True)

    for i in range(len(final_df['id'])):
        final_df.loc[i, 'id'] = final_df['id'][i][:17]
        
    reels_full = pd.merge(df, final_df, on='id', how='right')
    
    reels_full.rename(columns={"Comentários":"comments", "Curtidas":"likes", "Reproduções iniciais":"plays", "Contas alcançadas":"reach", "Salvo":"saved", "Compartilhamentos":"shares", "Interações com reels":"total_interactions"}, inplace=True)
    reels_full['date_etl'] = currentdate
    reels_full['date_etl'] = pd.to_datetime(reels_full['date_etl'], format="%Y-%m-%d")
    
    schema_data = [
        bigquery.SchemaField('caption','STRING'),
        bigquery.SchemaField('comments_count','INT64'),
        bigquery.SchemaField('like_count','INT64'),
        bigquery.SchemaField('media_product_type','STRING'),
        bigquery.SchemaField('media_type','STRING'),
        bigquery.SchemaField('media_url','STRING'),
        bigquery.SchemaField('permalink','STRING'),
        bigquery.SchemaField('timestamp','DATETIME'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('date_etl','DATE'),
        bigquery.SchemaField('comments','INT64'),
        bigquery.SchemaField('likes','INT64'),
        bigquery.SchemaField('plays','INT64'),
        bigquery.SchemaField('reach','INT64'),
        bigquery.SchemaField('saved','INT64'),
        bigquery.SchemaField('shares','INT64'),
        bigquery.SchemaField('total_interactions','INT64'),
    ]

    job_config_data = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_data)

    foi = client.load_table_from_dataframe(reels_full,f'{database}.reels', job_config=job_config_data)
          
    return foi.result

def query_carousel(df_total ,base, version, limite_postagens, access_token, database):
    data_carousel_total = []
    for x in df_total[(df_total.media_product_type == 'FEED') & (df_total.media_type == 'CAROUSEL_ALBUM')]['id']:
        tmp = get(f"{base}/{version}/{x}/insights?metric=total_interactions%2Cimpressions%2Creach%2Csaved%2Cvideo_views&limit={limite_postagens}&access_token={access_token}")
        tmp = tmp.json()
        if tmp is not None:
            data_carousel_total.append(tmp)
        else:
            pass
    dfs = []

    for entry in data_carousel_total:
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
        final_df.loc[i, 'id'] = final_df['id'][i][:17]
        
    carousel_full = pd.merge(df_total,final_df,on='id',how='right')
    
    carousel_full.rename(columns={"Impressões":"impressions",
                                  "Alcance":"reach",
                                  "Salvos":"saved",
                                  "Visualizações de vídeo":"video_views",
                                  "Interações com publicações":"engagement"},
                         inplace=True)
    carousel_full['date_etl'] = currentdate
    carousel_full['date_etl'] = pd.to_datetime(carousel_full['date_etl'], format="%Y-%m-%d")
    
    schema_data = [
        bigquery.SchemaField('caption','STRING'),
        bigquery.SchemaField('comments_count','INT64'),
        bigquery.SchemaField('like_count','INT64'),
        bigquery.SchemaField('media_product_type','STRING'),
        bigquery.SchemaField('media_type','STRING'),
        bigquery.SchemaField('media_url','STRING'),
        bigquery.SchemaField('permalink','STRING'),
        bigquery.SchemaField('timestamp','DATETIME'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('date_etl','DATE'),
        bigquery.SchemaField('engagement','INT64'),
        bigquery.SchemaField('impressions','INT64'),
        bigquery.SchemaField('reach','INT64'),
        bigquery.SchemaField('saved','INT64'),
        bigquery.SchemaField('video_views','INT64'),
    ]
    
    job_config_data = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_data)

    foi = client.load_table_from_dataframe(carousel_full,f'{database}.carousel', job_config=job_config_data)
    
    return foi.result

def query_image(df_total, base, version, limite_postagens, access_token, database):
    data_image_total = []
    for x in df_total[(df_total.media_type == 'IMAGE') & (df_total.media_product_type == 'FEED')]['id']:
        tmp = get(f"{base}/{version}/{x}/insights?metric=total_interactions%2Cimpressions%2Creach%2Csaved%2Cfollows%2Cprofile_visits%2Cshares%2Cprofile_activity&limit={limite_postagens}&access_token={access_token}")
        tmp = tmp.json()
        if tmp is not None:
            data_image_total.append(tmp)
        else:
            pass
    dfs = []

    for entry in data_image_total:
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
        final_df.loc[i, 'id'] = final_df['id'][i][:17]
        
    image_full = pd.merge(df_total,final_df,on='id',how='right')
    
    image_full.rename(columns={"Impressões":"impressions",
                                  "Contas alcançadas":"reach",
                                  "Salvo":"saved",
                                  "Compartilhamentos":"shares",
                                  "Interações com publicações":"engagement",
                                  "Atividade do perfil":"profile_activity",
                                  "Começaram a seguir":"follows",
                                  "Visitas ao perfil":"profile_visits"},
                         inplace=True)
    image_full['date_etl'] = currentdate
    image_full['date_etl'] = pd.to_datetime(image_full['date_etl'], format="%Y-%m-%d")
    
    
    schema_data = [
        bigquery.SchemaField('caption','STRING'),
        bigquery.SchemaField('comments_count','INT64'),
        bigquery.SchemaField('like_count','INT64'),
        bigquery.SchemaField('media_product_type','STRING'),
        bigquery.SchemaField('media_type','STRING'),
        bigquery.SchemaField('media_url','STRING'),
        bigquery.SchemaField('permalink','STRING'),
        bigquery.SchemaField('timestamp','DATETIME'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('date_etl','DATE'),
        bigquery.SchemaField('engagement','INT64'),
        bigquery.SchemaField('impressions','INT64'),
        bigquery.SchemaField('reach','INT64'),
        bigquery.SchemaField('saved','INT64'),
        bigquery.SchemaField('follows','INT64'),
        bigquery.SchemaField('profile_visits','INT64'),
        bigquery.SchemaField('shares','INT64'),
        bigquery.SchemaField('profile_activity','INT64'),
    ]
    
    job_config_data = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_data)

    foi = client.load_table_from_dataframe(image_full,f'{database}.image', job_config=job_config_data)
    
    return foi.result

def query_video_tv(df_total, base, version, limite_postagens, access_token, database):
    data_video_total = []
    for x in df_total[(df_total['permalink'].str.contains('tv')) & (df_total.media_product_type == 'FEED') & (df_total.media_type == 'VIDEO')]['id']:
        tmp = get(f"{base}/{version}/{x}/insights?metric=video_views%2Cimpressions%2Creach%2Csaved&limit={limite_postagens}&access_token={access_token}")
        tmp = tmp.json()
        if tmp is not None:
            data_video_total.append(tmp)
        else:
            pass
    dfs = []

    for entry in data_video_total:
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
        final_df.loc[i, 'id'] = final_df['id'][i][:17]
        
    video_full = pd.merge(df_total,final_df,on='id',how='right')
    
    video_full.rename(columns={"Impressões":"impressions",
                                  "Alcance":"reach",
                                  "Salvos":"saved",
                                  "Visualizações de vídeo":"video_views"},
                         inplace=True)
    video_full['date_etl'] = currentdate
    video_full['date_etl'] = pd.to_datetime(video_full['date_etl'], format="%Y-%m-%d")
    
    
    schema_data = [
        bigquery.SchemaField('caption','STRING'),
        bigquery.SchemaField('comments_count','INT64'),
        bigquery.SchemaField('like_count','INT64'),
        bigquery.SchemaField('media_product_type','STRING'),
        bigquery.SchemaField('media_type','STRING'),
        bigquery.SchemaField('media_url','STRING'),
        bigquery.SchemaField('permalink','STRING'),
        bigquery.SchemaField('timestamp','DATETIME'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('date_etl','DATE'),
        bigquery.SchemaField('impressions','INT64'),
        bigquery.SchemaField('reach','INT64'),
        bigquery.SchemaField('saved','INT64'),
        bigquery.SchemaField('video_views','INT64'),
    ]
    
    job_config_data = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_data)

    foi = client.load_table_from_dataframe(video_full,f'{database}.video_tv', job_config=job_config_data)
    
    
    return foi.result

def query_video_p(df_total, base, version, limite_postagens, access_token, database):
    data_video_total = []
    for x in df_total[(df_total['permalink'].str.contains('p')) & (df_total.media_product_type == 'FEED') & (df_total.media_type == 'VIDEO')]['id']:
        tmp = get(f"{base}/{version}/{x}/insights?metric=impressions%2Creach%2Csaved%2Cvideo_views%2Ccomments%2Cfollows%2Clikes%2Cprofile_activity%2C profile_visits%2Cshares%2Ctotal_interactions&limit={limite_postagens}&access_token={access_token}")
        tmp = tmp.json()
        if tmp is not None:
            data_video_total.append(tmp)
        else:
            pass
    dfs = []

    for entry in data_video_total:
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
        final_df.loc[i, 'id'] = final_df['id'][i][:17]
        
    video_full = pd.merge(df_total,final_df,on='id',how='right')
    video_full.drop(columns=['Comentários', 'Curtidas'], inplace=True)
    
    video_full.rename(columns={"Impressões":"impressions",
                                  "Contas alcançadas":"reach",
                                  "Salvo":"saved",
                                  "Visualizações de vídeo":"video_views",
                                  "Atividade do perfil":"profile_activity",
                                  "Interações com publicações":"engagement",
                                  "Começaram a seguir":"follows",
                                  "Visitas ao perfil":"profile_visits",
                                  "Compartilhamentos":"shares"},
                         inplace=True)
    video_full['date_etl'] = currentdate
    video_full['date_etl'] = pd.to_datetime(video_full['date_etl'], format="%Y-%m-%d")
    
    
    schema_data = [
        bigquery.SchemaField('caption','STRING'),
        bigquery.SchemaField('comments_count','INT64'),
        bigquery.SchemaField('like_count','INT64'),
        bigquery.SchemaField('media_product_type','STRING'),
        bigquery.SchemaField('media_type','STRING'),
        bigquery.SchemaField('media_url','STRING'),
        bigquery.SchemaField('permalink','STRING'),
        bigquery.SchemaField('timestamp','DATETIME'),
        bigquery.SchemaField('id','STRING'),
        bigquery.SchemaField('date_etl','DATE'),
        bigquery.SchemaField('impressions','INT64'),
        bigquery.SchemaField('reach','INT64'),
        bigquery.SchemaField('saved','INT64'),
        bigquery.SchemaField('video_views','INT64'),
        bigquery.SchemaField('follows','INT64'),
        bigquery.SchemaField('engagement','INT64'),
        bigquery.SchemaField('profile_activity','INT64'),
        bigquery.SchemaField('profile_visits','INT64'),
        bigquery.SchemaField('shares','INT64'),
    ]
    
    job_config_data = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_data)

    foi = client.load_table_from_dataframe(video_full,f'{database}.video_p', job_config=job_config_data)
    
    
    return foi.result









