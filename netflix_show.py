
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col,rank
spark = SparkSession.builder.getOrCreate()

raw_credits = spark.read.csv('/Users/vishnubharathreddyvankireddy/Downloads/raw_credits.csv',header= True)

raw_titles = spark.read.csv('/Users/vishnubharathreddyvankireddy/Downloads/raw_titles.csv',header= True)

#raw_titles.show()
raw_titles.columns

raw_credits.count()
raw_credits.head(1)
raw_credits.select(col('role')).distinct().show()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col,rank

joining = raw_credits.join(raw_titles, raw_credits["id"] == raw_titles["id"])
joining = joining.filter(col('role') == "ACTOR" )
joining = joining.filter(col('imdb_score').isNotNull())
joining.select(col('type')).distinct().show()
joining_show = joining.filter(col('type')=="SHOW")
joining_show.count()

joining_show = joining_show.select(col('name'), col('role'), col('title'), col('release_year'), col('imdb_score'), col('type'))
grouped_show = joining_show.groupBy(col('release_year'), col('name'), col('role'), col('title'), col('imdb_score'),col('type')).agg({"imdb_score": "max"})
# grouped_show.show(5)


grouped_show.withColumnRenamed('max(imdb_score)','imdb_score')
grouped_show = grouped_show.drop('max(imdb_score)')
partition_data = Window.partitionBy("release_year").orderBy("imdb_score")

#we can use rownumber for getting sequence of ranks for each row if match or use rank to give equal priority for all matching rows
grouped_show = grouped_show.select("*", row_number().over(partition_data).alias("Rank_row")).where(col("Rank_row") < 2)
# grouped_show.show(100)

grouped_show.orderBy('release_year')
grouped_show.show(10)

