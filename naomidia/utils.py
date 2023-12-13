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

base = configg['base'][0]
version = configg['version'][0]
ig_user_id = configg['ig_user_id_br'][0]
limite_postagens = configg['limite_postagens'][0]
access_token = configg['access_token'][0]



def query_demographics(base, version, ig_user_id, access_token, database):
    detalhamento = ['age', 'city', 'gender', 'country']

    metrica = ['engaged_audience_demographics', 'reached_audience_demographics', 'follower_demographics']

    timeframe = ['last_14_days', 'last_30_days', 'last_90_days', 'prev_month', 'this_month', 'this_week']

    df = pd.DataFrame()
    for i in detalhamento:
        for j in timeframe:
            for k in metrica:
                try:
                    rel = get(f"{base}/{version}/{ig_user_id}/insights?metric={k}&period=lifetime&breakdown={i}&metric_type=total_value&timeframe={j}&access_token={access_token}")

                    # Verificar se a resposta não é nula
                    if rel is not None and rel.status_code == 200:
                        rel = rel.json()
                        with open("instagram_relll.json", "w") as file:
                            file.write(json.dumps(rel, indent=4))
                        rel = open("instagram_relll.json")
                        rel = json.load(rel)

                        # Verificar se 'data' está presente nos dados
                        if 'data' in rel:
                            rel = rel['data']
                            tmp = pd.DataFrame(rel[0]['total_value']['breakdowns'][0]['results']).sort_values(by=['value'], ascending=False).reset_index(drop=True)
                            tmp['dimension_values'] = tmp['dimension_values'].apply(lambda x: x[0].replace('[', '').replace(']', ''))
                            tmp['detalhamento'] = i
                            tmp['timeframe'] = j
                            tmp['metrica'] = k
                            tmp['date'] = currentdate
                            tmp['date'] = pd.to_datetime(tmp['date'], format="%Y-%m-%d", errors='coerce')
                            df = pd.concat([tmp, df]).reset_index(drop=True)
                        else:
                            pass
                            # Pode adicionar mais lógica aqui, como definir valores padrão ou registrar o erro
                    else:
                        pass
                except Exception as e:
                    print(f"Erro ao processar {i}, {j}, {k}: {str(e)}")
                    
    schema_rel = [
        bigquery.SchemaField('dimension_values','STRING'),
        bigquery.SchemaField('value','INT64'),
        bigquery.SchemaField('detalhamento','STRING'),
        bigquery.SchemaField('timeframe','STRING'),
        bigquery.SchemaField('metrica','STRING'),
        bigquery.SchemaField('date','DATE'),
    ]

    job_config_rel = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_rel)

    foi = client.load_table_from_dataframe(df,f'{database}.demographics', job_config=job_config_rel)
    return foi.result


def query_dailymetrics(base, version, ig_user_id, access_token, database):
    rel2 = get("{}/{}/{}/insights?metric=reach%2Cprofile_views%2Cfollower_count&period=day&access_token={}".format(base, version, ig_user_id, access_token))
    rel2 = rel2.json()
    with open("instagram_rel2.json", "w") as file:
        file.write(json.dumps(rel2, indent = 4))
    rel2 = open("instagram_rel2.json") 
    rel2 = json.load(rel2)
    rel2 = rel2['data']
    df = pd.DataFrame(rel2)
    df['values'][0] = df['values'][0][0]['value']
    df['values'][1] = df['values'][1][0]['value']
    df['values'][2] = df['values'][2][0]['value']
    df = pd.DataFrame(df).T
    df.drop(['period', 'title', 'description', 'id', 'name'], inplace = True)    
    df.rename({0: 'reach', 1: 'profile_views', 2: 'follower_count'}, axis=1, inplace=True)  
    df['date'] = currentdate
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d", errors='coerce')
    
    schema_rel = [
        bigquery.SchemaField('reach','INT64'),
        bigquery.SchemaField('profile_views','INT64'),
        bigquery.SchemaField('follower_count','INT64'),
        bigquery.SchemaField('date','DATE'),
    ]

    job_config_rel = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema_rel)

    foi = client.load_table_from_dataframe(df,f'{database}.dailymetrics', job_config=job_config_rel)
                            
    return foi.result

        
def query_follow_total(base, version, ig_user_id,access_token, database):
    follow_total = get("{}/{}/{}?fields=followers_count&access_token={}".format(base, version, ig_user_id, access_token))
    follow_total = follow_total.json()
    with open("follow_total.json", "w") as file:
        file.write(json.dumps(follow_total, indent = 4))
    follow_total = open("follow_total.json")
    follow_total = json.load(follow_total)
    follow_total = pd.DataFrame.from_dict(follow_total, orient='index').T
    follow_total = follow_total['followers_count']
    df = pd.DataFrame(follow_total)
    df['date'] = currentdate
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d", errors='coerce')
    
    schema = [
        bigquery.SchemaField('followers_count','INT64'),
        bigquery.SchemaField('date','DATE'),
    ]

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, schema = schema)

    foi = client.load_table_from_dataframe(df,f'{database}.dailyfollowers', job_config=job_config)
                            
    return foi.result
