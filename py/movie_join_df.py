from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("joinDF").getOrCreate()

load_dt=sys.argv[1]

df=spark.read.parquet(f"/home/root2/data/movie/repartition/load_dt={load_dt}")
df.createOrReplaceTempView("movie")

df_m=spark.sql("SELECT * FROM movie WHERE repNationCd IS NULL")
df_n=spark.sql("SELECT * FROM movie WHERE multiMovieYn IS NULL")

df_m.createOrReplaceTempView("movie_m")
df_n.createOrReplaceTempView("movie_n")

col_set=""
for i in df_m.columns:
    col_set+=f"NVL(m.{i},n.{i}) {i},"

query=f"SELECT {load_dt} load_dt, {col_set[:-1]} FROM movie_m m FULL JOIN movie_n n ON m.movieCd=n.movieCd"

df_join=spark.sql(query)

df_join.show(50)

# df_join.write.mode('append').partitionBy("load_dt", "multiMovieYn", "repNationCd").parquet("/home/root2/data/movie/hive")
df_join.write.mode('overwrite').partitionBy("multiMovieYn", "repNationCd").parquet(f"/home/root2/data/movie/hive/load_dt={load_dt}")

spark.stop()
