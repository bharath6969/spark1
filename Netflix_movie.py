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


#joining two dataframes based on id 
joining = raw_credits.join(raw_titles, raw_credits["id"] == raw_titles["id"])


#filtering joining based on ACTOR role and fill the imdb_score to 0 if null
joining = joining.filter(col('role') == "ACTOR" )
joining = joining.filter(col('imdb_score').isNotNull())

#checking the types available in dataframe
joining.select(col('type')).distinct().show()

#filteringf my dataframe with movies
joining_show = joining.filter(col('type')=="MOVIE")


# selecting and using groupby function based on years and using aggreation function on imdb_score as max value 
joining_show = joining_show.select(col('name'), col('role'), col('title'), col('release_year'), col('imdb_score'), col('type'))
grouped_show = joining_show.groupBy(col('release_year'), col('name'), col('role'), col('title'), col('imdb_score'),col('type')).agg({"imdb_score": "min"})
# grouped_show.show(5)

#we are renaming the max(imdb_score to imdb_score for clear understanding 
grouped_show.withColumnRenamed('min(imdb_score)','imdb_score')

# removed the max(imdb_score) column
grouped_show = grouped_show.drop('min(imdb_score)')


#window function to partitin the data and use the row_number or rank methods for ranking the data 
partition_data = Window.partitionBy("release_year").orderBy(col('imdb_score').desc())

#we can use rownumber for getting sequence of ranks for each row if match or use rank to give equal priority for all matching rows
grouped_show = grouped_show.select("*", row_number().over(partition_data).alias("Rank_row")).where(col("Rank_row") <2)
# grouped_show.show(10)

# listing the dataframe based o sequence order of release_year and rank_row
grouped_show.orderBy(col('release_year').asc())
grouped_show.show(100)



