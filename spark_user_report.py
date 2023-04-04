from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, current_timestamp, from_unixtime
from pyspark.sql import functions as F
spark= SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)])

schema1 = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("timestamp", StringType(), True)
])
schema3 = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("file_name", StringType(), True),
    StructField("timestamp", StringType(), True)
])
user_dump_data=spark.read.format("csv").option("header", "true").schema(schema3).load("/user/hadoop/vishnu/user_dump_hive.csv/user_dump.csv")
user_data= spark.read.format("csv").option("header", "true").schema(schema).load("/user/hadoop/vishnu/user.csv")

user_activity= spark.read.format("csv").option("header", "true").schema(schema1).load("/user/hadoop/vishnu/user_activity_hive/vishnu_user_activity.csv")

# user_data = spark.read.option('csv('/Users/vishnubharathreddyvankireddy/Downloads/Reddy_user.csv',header = True)
# user_activity = spark.read.csv('/Users/vishnubharathreddyvankireddy/Downloads/vishnu_bharath_activity.csv',header = True)
user_data.createTempView('user_data_table')
user_activity.createTempView('activity_data_table')
last_activity_type = spark.sql('select b.user_id,a.name ,b.event_id, b.type,b.timestamp from activity_data_table as b LEFT JOIN user_data_table as a where a.id == b.user_id')
convert_timestamp_dataframe = last_activity_type.select(col('user_id').alias('last_activity_user_id'),
                                                       col('type').alias('last_type'),
                                                        col('event_id').alias('even_id'),
                                                       col('timestamp').alias('latest_timestamp'),
                                                       col('name').alias('latest_name'))
timestamp_sorted= convert_timestamp_dataframe.orderBy(desc('latest_timestamp'))
last_activity_type.show(3)
#user_dump_data = spark.read.csv('/Users/vishnubharathreddyvankireddy/Downloads/user_upload_dump_2023_03_06.csv',header = True)
user_report = timestamp_sorted.groupBy('last_activity_user_id','last_type').agg(count("*").alias('total_operations'))
user_report= user_report.withColumn('total_updates',when(col('last_type')== "UPDATE",col('total_operations')).otherwise(0))
user_report= user_report.withColumn('total_inserts',when(col('last_type')== "INSERT",col('total_operations')).otherwise(0))
user_report= user_report.withColumn('total_delete',when(col('last_type')== "DELETE",col('total_operations')).otherwise(0))
updated_user_report=user_report.groupBy('last_activity_user_id').agg(sum('total_updates').alias('Total_updates'),
                                sum('total_inserts').alias('Total_inserts'),
                                sum('total_delete').alias('Total_deletes'))
timestamp_sorted.createTempView('latest_timestamp')
m = spark.sql('SELECT last_activity_user_id, last_type, MAX(latest_timestamp) AS latest_timestamp FROM latest_timestamp GROUP BY last_activity_user_id, last_type')
m.createTempView('find_latest_timestamp1')
grouping_m_table = spark.sql('SELECT last_activity_user_id, MAX(latest_timestamp) as latest_timestamp1 FROM find_latest_timestamp1 GROUP BY last_activity_user_id')
grouping_m_table.createTempView('table1')
m.createTempView('table2')
Join_tables = spark.sql('select a.last_activity_user_id,a.latest_timestamp1,b.last_type from table1 as a ,table2 as b where a.last_activity_user_id==b.last_activity_user_id and a.latest_timestamp1 == b.latest_timestamp')
Join_tables.createTempView('table6')
updated_user_report.createTempView('table5')
Final_aggregation = spark.sql('select a.last_activity_user_id,a.Total_updates,a.Total_inserts,a.Total_deletes,b.last_type,b.latest_timestamp1 from table5 as a,table6 as b where b.last_activity_user_id==a.last_activity_user_id')
new_join= Final_aggregation.select(
     col('last_activity_user_id'),
    col('Total_updates'),
    col('Total_inserts'),
    col('Total_deletes'),
    col('last_type'),
    col('latest_timestamp1'),
    current_timestamp().alias('current_time'),
    (current_timestamp() - from_unixtime(col('latest_timestamp1').cast('bigint')).cast('timestamp')).alias('timedifference')
)
new_join = new_join.withColumn('is_greater_than_2_days', when(datediff(current_timestamp(), (current_timestamp() - col('timedifference'))) < 2, True).otherwise(False))
new_join.show(2)
user_dump_data = user_dump_data.groupBy('user_id').agg(count("*").alias('total_upload_counts'))
user_dump_data.show(2)

user_dump_data.createTempView('user_dump_table')
new_join.createTempView('upload_connect')

Final_user_report = spark.sql('select a.last_activity_user_id,a.Total_updates,a.Total_inserts,a.Total_deletes,a.last_type,a.latest_timestamp1,a.is_greater_than_2_days,b.total_upload_counts from  upload_connect as a,user_dump_table as b where a.last_activity_user_id == b.user_id')
Final_user_report.show()

spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS project.final_user_report (last_activity_user_id INT,Total_updates INT,Total_inserts INT,Total_deletes INT,last_type STRING,latest_timestamp1 TIMESTAMP,is_greater_than_2_days BOOLEAN,total_upload_counts INT)\
        ROW FORMAT DELIMITED\
        FIELDS TERMINATED BY ','\
        STORED AS TEXTFILE\
        LOCATION '/user/hadoop/vishnu/final_report.csv'")





Final_user_report.write.mode("overwrite").saveAsTable("project.final_user_report")

