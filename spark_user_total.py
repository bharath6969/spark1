from pyspark.sql.functions import col, coalesce, count, current_timestamp,lit
from pyspark.sql import SparkSession

spark = SparkSession.builder.config("spark.sql.warehouse.dir","/user/hive/warehouse").enableHiveSupport().getOrCreate()
df = spark.table("project.user_data_hive")
df1 = df.select(current_timestamp().alias("time_ran"), count("*").alias("total_users"))
df2 = spark.table("project.user_total").select(col("time_ran"), col("total_users"))
final = df1.join(df2, df1["time_ran"] > df2["time_ran"], "left") \
           .select(df1["time_ran"], df1["total_users"], (df1["total_users"] - coalesce(df2["total_users"], lit(0))).alias("users_added")) \
           .orderBy(df1["time_ran"])

#format("hive") because by default it would be "parquet"
final.write.mode("append").format("hive").saveAsTable("project.user_total")
