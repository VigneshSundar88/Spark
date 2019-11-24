from pyspark.sql.functions import substring

class parse_file():
    def __init__(self):
        self.fixed_length_schema = "https://github.com/VigneshSundar88/Spark/blob/master/Dataset/fixed_data.csv"
        self.fixed_length_file = "https://github.com/VigneshSundar88/Spark/blob/master/Dataset/fixed_length_file.txt"
      
    def create_df(self):
        read_fixed_schema = spark.read.format('csv').\
            option("header", "true").\
            option("inferSchema", "true").\
            load(self.fixed_length_schema)
 
        read_fixed_length_file = spark.read.format("csv").\
            option("header", "false").\
            load(self.fixed_length_file).\
            toDF("fixed_line")
           
        self.substr_file(read_fixed_schema, read_fixed_length_file) 
  
    def substr_file(self, read_fixed_schema, read_fixed_length_file):
        read_fixed_schema.show()
        schema_as_dict = map(lambda schema_to_dict: schema_to_dict.asDict(), read_fixed_schema.collect())
        create_sub_df = read_fixed_length_file.\
            select(*[
                    substring(
                    str = 'fixed_line',
                    pos = int(header['start']),
                    len = int(header['length'])).\
                    alias(header['Col_Name'])
                    for header in schema_as_dict
                    ]).show()

if __name__ == "__main__":
    parse_rec = parse_file()
    parse_rec.create_df()
