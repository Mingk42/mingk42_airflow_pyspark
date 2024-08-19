from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("select_parquet").getOrCreate()

home_path=os.path.expanduser("~")

df = spark.read.parquet(f"{home_path}/data/movies/parquet/movieList.parquet")

cntByPeople=df.groupby("peopleNm").count()
cntByPeople.createOrReplaceTempView("cbp")
cntByPeople=spark.sql("SELECT * FROM cbp ORDER BY count DESC")
cntByPeople.show()

cntByCompany=df.groupby("companyNm").count()
cntByCompany.createOrReplaceTempView("cbc")
cntByCompany=spark.sql("SELECT * FROM cbc ORDER BY count DESC")
cntByCompany.show()

spark.stop()
