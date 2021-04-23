from pyspark.sql.types import *

# Read JSON using schema that is present in the JSON file:
# ========================================================
readJsonSchemaJson = spark.read.json(<schema.json>)
fetchJsonSchema = readJsonSchemaJson.schema

readJsonWithSchema = spark.read.schema(fetchJsonSchema).json(<source.json>)

# Read CSV using schema:
# ======================
csvFileSchema = StructType([StructField("col_1", StringType(), True),
StructField("col_2", StringType(), True)])

readCSVWithSchema = spark.read.csv("<csv_file_path>", sep='|', schema=csvFileSchema)
