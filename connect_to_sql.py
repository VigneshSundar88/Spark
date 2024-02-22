from pyspark.dbutils import DBUtils
from msal import ConfidentialClientApplication

def connect_to_sql(env: str) -> str:
  env_var = env
  client_id = dbutils.secrets.get('<kv_secret_name>')
  client_secret = dbutils.secrets.get('<kv_secret_name>')
  tenant_id = dbutils.secrets.get('<kv_secret_name>')

  authority = f"https://login.microsoftonline.com/{tenant_id}"
  app = ConfidentialClientApplication(
    client_id,
    authority = authority
    client_credential = client_secret
  )

  result = app.acquire_token_for_client(scopes=["https://database.windows.net/.default"])
  access_token = result['access_token']

  server = '<sql_server_name>'
  database = '<db_name>'

  url = '<sql_conn_url>'
  
  return url, access_token

url, access_token = connect_to_sql('dev')

df_sql = (
  spark.read
  .format("jdbc")
  .option("url", <url>)
  .option("dbtable", <raw_table>)
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("accessToken", <access_token>)
  .load()
)
