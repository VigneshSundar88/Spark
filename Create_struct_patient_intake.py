from pyspark.sql.functions import collect_list, struct, col

patient_val = [(123, "XYZ", 1, "A"), (123, "XYZ", 1, "B"), (123, "XYZ", 2, "C"), (123, "XYZ", 2, "D")]
patient_df = spark.createDataFrame(test_val, ["PatientNumber", "PatientSSN", "PatientIntakeId", "PatientSubscriberId"])

create_array_df = patient_df.groupBy("PatientNumber", "PatientSSN", "PatientIntakeId").agg(collect_list(col("PatientSubscriberId")).alias("PatientSubIdArray"))
create_struct_df = create_array_df.groupBy("PatientNumber", "PatientSSN").agg(struct(col("PatientIntakeId"), col("PatientSubIdArray")))

create_struct_df.show()