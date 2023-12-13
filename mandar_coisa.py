import os
from google.cloud import bigquery
import pandas_gbq
import pandas as pd

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecredentials.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecredentials.json"
project_id = 'datalake-2022'
client = bigquery.Client()

facebook_se_liga_id =  109202651936229 
access_token_facebook_se_liga = 'EAARcGZBCPaSEBO78oBZBwdyILVG4mKsscLP8E51SLnAayTWWwk4ZA3FpAAa6UvH9UackaUa6MVAJUGBBaAggthtCIxqOhkyudF6RjMmGSzXxkUWlVvlzGTjFujg7VVirZBeVAKnH5QR0EI5h1IonOwpRnChavn203u17kMobZBnI4MjWRcY9CZCpTRtuPn'
facebook_fis_id = 852845231759667 
access_token_facebook_fis = 'EAARcGZBCPaSEBO4rRUhIvFHwbqTvwlZBMIhKTCCUDTY0QxFb4okwZAZCleXfZAZAgFZCZA8meCbcL6CUFOWpWITViqEzHQK5AZA8kO9WtgZCNYFPpBxBGYw3hWJO3HNqdK5ZAqrIw7FNtNT5uneAScacwWONe9fs7j9avsURsoYxi1Cdsn7DDFxbDyiE8fuOZAmsTAZDZD'
facebook_latam_id = 109083291949351 
access_token_facebook_latam = 'EAARcGZBCPaSEBO4G1Mtg5EazLlCmS311JHFMZBZBupwt6w26GiGnlH2jtEXQek6toc5zfTHlWAkxHFMUKz9VLZBLzWdlNVQNm68pJU8qHNWsGwZAAW783r95wlUAAKfYkxe35AUdsUj8xxun2SnWKElSYnxoNyUCKteTMtXcqARJQZAPLGr0UlIzpS8XRe'
base = 'https://graph.facebook.com/'
limit = 100


follow = {'facebook_se_liga_id' : facebook_se_liga_id,
          'facebook_fis_id' : facebook_fis_id,
          'facebook_latam_id' : facebook_latam_id,
          'access_token_facebook_se_liga' : access_token_facebook_se_liga,
        'access_token_facebook_fis' : access_token_facebook_fis,
    'access_token_facebook_latam' : access_token_facebook_latam,
    'base' : base,
    'limit' : limit
    }  


follow = pd.DataFrame.from_dict(follow, orient='index').T

job = client.load_table_from_dataframe(follow,'variaveis_de_ambiente.config_for_etl_facebook')




access_token = 'EAAFYlVUbH3cBO03DOeKLQGkZBDZB9krTqlBVfmnxcnPW3JpH8QY4cyeQZA9CFqY3PSabOYAVQxjumIRVBuWMkvxIt31yf60076AZB7P7W1ExVoKOQWHIyMCeG8On6csGe2sQXQO1rB4wlLvhz8AIjBZB2XC23nOzEP6SSoqq2rcfvpdBnJV1aCp5QSm6LUgic'
business_id = '852845231759667'
ig_user_id_br = '17841408841693316'
ig_user_id_latam = '17841450884332431'
ig_user_id_seliga = '17841452302292722'
ad_account = 'act_697816211148710'

base = "https://graph.facebook.com/"
version = "v18.0/"
#limite_comentarios = 10
limite_postagens = 200000


follow2 = {'access_token' : access_token,
          'business_id' : business_id,
          'ig_user_id_br' : ig_user_id_br,
          'ig_user_id_latam' : ig_user_id_latam,
        'ig_user_id_seliga' : ig_user_id_seliga,
    'ad_account' : ad_account,
    'base' : base,
    'version' : version,
   'limite_postagens' : limite_postagens
    }  


follow2 = pd.DataFrame.from_dict(follow2, orient='index').T

job = client.load_table_from_dataframe(follow2,'variaveis_de_ambiente.config_for_etl_instagram')

