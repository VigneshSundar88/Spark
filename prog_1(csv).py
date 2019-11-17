#import pyspark.sql.SparkSession
#import ConfigParser
from pyspark.sql.types import *

myManualSchema = StructType([
StructField("CustomerName", StringType(), True),
StructField("DOB", StringType(), True),
StructField("SSN", StringType(), False),
StructField("MailID", StringType(), False),
StructField("PhoneNumber", StringType(), False),
StructField("City", StringType(), False),
StructField("State", StringType(), False),
StructField("ZipCode", LongType(), False),
StructField("CreditLimit", LongType(), False)])

data_source_csv = "/FileStore/tables/CreditCardApplicationData.csv"

data_source_csv_corrupted = "/FileStore/tables/CreditCardApplicationData_corrupted.csv"

read_csv = spark.read.format("csv").\
option("mode", "dropMalformed").\
option("inferschema", "true").\
schema(myManualSchema).\
load(data_source_csv)

read_csv.show(5)