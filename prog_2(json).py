import configparser
import threading
import multiprocessing
from pyspark.sql.types import StructType, StructField, StringType, LongType

class comp_tbl(object):
    def __init__(self):
        self.data_source_json = "/FileStore/tables/new_reviews-ddfcb.json"
        self.data_target_json = "/FileStore/tables/new_reviews-ddfcb.json"
        
    def create_source_df_tbl(self):
        json_source_df = spark.read.format("json").\
        option("inferSchema", "true").\
        load(self.data_source_json)
        #print(read_json)
        #read_json.show()
        df_source_tbl = json_source_df.write.mode("overwrite").saveAsTable("json_to_source_table")
        #self.query_table(df_source_tbl)
		
    def create_target_df_tbl(self):
        json_target_df = spark.read.format("json").\
        option("inferschema", "true").\
        load(self.data_target_json)
        df_target_tbl = json_target_df.write.mode("overwrite").saveAsTable("json_to_target_table")
        #self.
        
    def query_table(self):
        query_1 = sql("select * from json_to_source_table where useful = 0")
        query_2 = sql("select * from json_to_target_table where useful = 1")
        #query_1.show()
        #read_source_tbl = spark.read
        query_1.subtract(query_2).show()
        
c = comp_tbl()
c.create_source_df_tbl()
c.create_target_df_tbl()
c.query_table()