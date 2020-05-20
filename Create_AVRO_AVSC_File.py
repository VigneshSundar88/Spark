from collections import OrderedDict
import json
from sys import argv
import subprocess as SP

from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

spark = SparkSession.\
		builder.\
		enableHiveSupport().\
		getOrCreate()

# A CSV file with only the column names are given, we have to infer the column names and schema from the file
read_meta_csv_file = spark.read.csv(argv[1], sep="|", header=True)
meta_column = read_meta_csv_file.columns

# Change column names with double underscore("__") to single underscore("_")
split_col_list = ["_".join(col_val.split("__")) for col_val in meta_column]

# Change the dataframe with the modified column names
change_meta_df = read_meta_csv_file.toDF(*split_col_list)

# Fetch the schema from the dataframe
tbl_schema = change_meta_df.schema

schema_datatype_dict = {StringType(): "string"}

# Build the AVSC schema, in the AVSC schema, the content is in the dictionary format
bdnc_schema = OrderedDict()
bdnc_schema['name'] = argv[2]
bdnc_schema['type'] = "record"
fields_list = []
for val in tbl_schema.fields:
	fields_ordered_dict = OrderedDict()
	fields_ordered_dict["name"] = val.name
	fields_ordered_dict["type"] = ["null", schema_datatype_dict[val.dataType]]
	fields_ordered_dict["default"] = None
	fields_list.append(fields_ordered_dict)
bdnc_schema["fields"] = fields_list

#Changing the schema as a JSON value
str_bdnc_schema = json.dumps(bdnc_schema)

#Write the output schema to a file
with open(argv[3], "w") as avro_schema:
	avro_schema.write(str_bdnc_schema)

#Copy the file to HDFS location
copy_avsc_local_hdfs = SP.Popen(["hdfs", "dfs", "-copyFromLocal", argv[3], argv[4]], stdout=SP.PIPE, stderr=SP.PIPE)
(copy_hdfs_file_output, copy_hdfs_file_error) = copy_avsc_local_hdfs.communicate()

#List the file in HDFS to see if the AVSC file is moved successfully
list_avsc_hdfs_file = SP.Popen(["hdfs", "dfs", "-ls", argv[4]], stdout=SP.PIPE, stderr=SP.PIPE)
(list_hdfs_avsc_file_output, list_hdfs_avsc_file_error) = list_avsc_hdfs_file.communicate()
print(list_hdfs_avsc_file_output)

#Create the AVRO file from the CSV data file
read_data_csv = spark.read.csv(argv[5], sep="|", header=True)
write_avro = read_data_csv.write.format("com.databricks.spark.avro").mode("overwrite").save(argv[6])

#List the AVRO files from the HDFS location
list_avro_data_file = SP.Popen(["hdfs", "dfs", "-ls", argv[6]], stdout=SP.PIPE, stderr=SP.PIPE)
(list_avro_file_output, list_avro_file_error) = list_avro_data_file.communicate()
print(list_avro_file_output)