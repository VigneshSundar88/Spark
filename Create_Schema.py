from pyspark.sql.types import StructField, StringType, IntegerType

def create_schema():
    fixed_length_schema = "/FileStore/tables/fixed_data.csv"
    read_fixed_schema = spark.read.format('csv').\
            option("header", "true").\
            option("inferSchema", "true").\
            load(fixed_length_schema)
    schema_as_dict = map(lambda schema_to_dict: schema_to_dict.asDict(), read_fixed_schema.select("Col_Name", "Datatype", "Nullable").collect())
    list_schema = list(schema_as_dict)
    datatype_dict = {'StringType()': StringType(), 'IntType()': IntegerType()}
    struct_field_list = []
    for schema_dict in list_schema:
        struct_field_list.append(StructField(schema_dict['Col_Name'], datatype_dict[schema_dict['Datatype']], schema_dict['Nullable']))
    final_struct = StructType(fields=struct_field_list)
    print(final_struct)
create_schema()