def get_env() -> str:
  tags = {vv["key"]:vv["value"] for vv in json.loads(spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags"))}
  return tags["environment"].lower()

def mount_blob():
  dbutils.fs.mount(
    source=f"<blob_connectivity_details>",
    mount_point = "/mnt/blobstorage",
    extra_configs = {"fs.azure.account.key.<storage_account>.fac.blob.core.windows.net": dbutils.fs.get("<kv_account>", "kv_name")}
  )

def mount_adls_gen2():
   dbutils.fs.mount(
    source=f"abfss_connectivity_details>",
    mount_point = "/mnt/adlsgen2storage",
    extra_configs = {"fs.azure.account.auth.type": "oAuth",
                    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    "fs.azure.account.oauth2.client.id": "<spn-id>",
                    "fs.azure.account.oauth2.client.secret": "<spn-key>",
                    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant_id>/oauth2/token"
                    }
  )
