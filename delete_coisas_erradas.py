from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googlecredentials.json"
project_id = 'datalake-2022'

client = bigquery.Client()

dml_statement1 = f"""DELETE FROM `datalake-2022.instagrambr.dailyfollowers` WHERE 'linha' = 1 """


# dml_statement1 = f"""DELETE FROM `datalake-2022.instagrambr.dailymetrics` WHERE profile_views < 10 """

query_job = client.query(dml_statement1)
query_job.result()