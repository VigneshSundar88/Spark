def filecrawler(path: str, suffix: str=".parquet", drop_dbfs: str = True) -> List[Dict[str, str]]:
  result = dbutils.fs.ls(path)

  out_files = [{"file": ff.path, "modificationtime_utc": ff.modificationTime} for ff in result if(ff.name.endswith(suffix) & ff.isFile())]
  dirs = [ff.path for ff in result if ff.isDir()]

  if drop_dbfs:
    out_files = [{"file":ff["file"].replace("dbfs:", ""), "modificationtime_utc": ff["modificationtime_utc"]} for ff in out_files]
    dirs = [dd.replace("dbfs:", "") for dd in dirs]

  out_dirs = [dbutils_filecrawler(dd, suffix=suffix, drop_dbfs=drop_dbfs) for dd in dirs]

  out_dirs = [ii for dir in out_dirs for ii in dir]

  return out_files+out_dirs

def convert_filelist_to_dataframe(files: List[Dict[str, str]]) -> DataFrame:
  df = spark.createDataFrame(files)
  df = df.withColumn("modificationtime_utc", F.col("modification_utc")/1000)
  df = df.withColumn("modificationtime_utc", F.to_utc_timestamp(F.from_unixtime("modificationtime_utc"), "UTC"))
  df = df.withColumn("modificationtime_est", F.from_utc_timestamp("modificationtime_utc", "EST"))
  return df

def get_latest_files() -> List[str]:
  file_list = filecrawler('<adls_path>')
  file_list = convert_filelist_to_dataframe(file_list)

  df = df.filter(F.col("modificationtime_est") >= ingestion_startdate)
  files = [ff["file"] for ff in df.select("file").collect()]
  return files
