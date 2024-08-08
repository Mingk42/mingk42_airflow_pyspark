"""SimpleApp.py"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("aggDF").getOrCreate()

df=spark.read.parquet("/home/root2/data/movie/hive/")

df.createOrReplaceTempView("movie_info")

col_set=""

for col in ['rnum', 'rank', 'rankInten', 'salesAmt', 'salesShare', 'salesInten', 'salesChange', 'salesAcc', 'audiCnt', 'audiInten', 'audiChange', 'audiAcc', 'scrnCnt', 'showCnt']:
    col_set+=f"SUM({col}) sum_{col},"

sumByMultiMovieYn=spark.sql(f"SELECT any_value(load_dt),multiMovieYn,{col_set[:-1]} FROM movie_info GROUP BY multiMovieYn")
sumByMultiMovieYn.write.mode("overwrite").partitionBy("load_dt").parquet("/home/root2/data/movie/sum-multi/")
sumByMultiMovieYn.show()

sumByRepNationCd=spark.sql(f"SELECT any_value(load_dt),repNationCd,{col_set[:-1]} FROM movie_info GROUP BY repNationCd")
sumByRepNationCd.write.mode("overwrite").partitionBy("load_dt").parquet("/home/root2/data/movie/sum-nation/")
sumByRepNationCd.show()


spark.stop()
