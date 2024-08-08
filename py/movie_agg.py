"""SimpleApp.py"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("joinDF").getOrCreate()

df=spark.read.parquet("/home/root2/data/movie/hive/")

df.createOrReplaceTempView("movie_info")

col_set=""

for col in ['rnum', 'rank', 'rankInten', 'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt']:
    col_set+=f"SUM({col}) sum_{col},"

sumByMultiMovieYn=spark.sql(f"SELECT any_value(load_dt),multiMovieYn,{col_set[:-1]} FROM movie_info GROUP BY multiMovieYn")
sumByMultiMovieYn.show()

sumByRepNationCd=spark.sql(f"SELECT any_value(load_dt),repNationCd,{col_set[:-1]} FROM movie_info GROUP BY repNationCd")
sumByRepNationCd.show()


spark.stop()
