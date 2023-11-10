#TOML document for Instagram ETL

from google.cloud import bigquery
import pandas as pd

config_for_etl_insta = dict()
config_for_etl_insta['access_token'] = 'EAARcGZBCPaSEBACBZCuIKFXiVFOZAPEjp0Vb1ZAs7hsPDQN25sZATEYfcuyrXFZBqFI9fgCHrg0z663pzrQ9hhbghQ3yES7IQ9pgG6PTmQphX6dvjlKuKtYhIlRrbQ6H6IOzpkcrH1CHUeYwDyqpSXmxr1fSNZCIg1rO9vDgK5QqAZDZD'
config_for_etl_insta['business_id'] = '852845231759667'
config_for_etl_insta['ig_user_id_br'] = '17841408841693316'
config_for_etl_insta['ig_user_id_latam'] = '17841450884332431'
config_for_etl_insta['ig_user_id_seliga'] = '17841452302292722'
config_for_etl_insta['ad_account'] = 'act_697816211148710'
config_for_etl_insta['base'] = "https://graph.facebook.com/"
config_for_etl_insta['version'] = "v12.0/"
config_for_etl_insta['limite_postagens'] = 20000

config_for_etl_insta = pd.DataFrame.from_dict(config_for_etl_insta, orient='index').T

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="googlecredentials.json"

project_id = 'datalake-2022'

client = bigquery.Client()

job = client.load_table_from_dataframe(config_for_etl_insta,'variaveis_de_ambiente.config_for_etl_insta')
