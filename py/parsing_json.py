from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, ArrayType
import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("parsing_json").getOrCreate()

home_path=os.path.expanduser("~")

def get_json_keys(schema, prefix):
    keys=[]
    for field in schema.fields:
        if isinstance(field.dataType, StructType):
            if prefix:
                new_prefix=f"{prefix}.{field.name}"
            else:
                new_prefix=field.name
            keys+=get_json_keys(field.dataType, new_prefix)
        elif isinstance(field.dataType, ArrayType) and isinstance(field.dataType.elementType, StructType):
            if prefix:
                new_prefix=f"{prefix}.{field.name}"
            else:
                new_prefix=field.name
            keys+=get_json_keys(field.dataType.elementType, new_prefix)
        else:
            if prefix:
                keys.append(f"{prefix}.{field.name}")
            else:
                keys.append(field.name)
    return keys


# pyspark 에서 multiline(배열) 구조 데이터 읽기
jdf = spark.read.option("multiline","true").json(f"{home_path}/data/movies/movieList/year=2023/data.json")

from pyspark.sql.functions import explode, col, size

# 펼치기
edf = jdf.withColumn("company", explode("companys"))

# 또 펼치기
eedf = edf.withColumn("director", explode("directors"))


k=get_json_keys(eedf.schema,"")
# 펼치기 전 column 삭제
for col in ['companys.companyCd', 'companys.companyNm', 'directors.peopleNm']:
    k.remove(col)
eedf.select(*k).show()

eedf.select(*k).write.mode("overwrite").parquet(f"{home_path}/data/movies/parquet/movieList.parquet")

"""
#def call(year, file_path):
def call():
    home_path=os.path.expanduser("~")
    df=spark.read.json(f"{home_path}/data/movies/movieList/year=2023/data.json")

    dynamic_schema = spark.read.json(df.rdd.map(lambda row : row.json_string)).schema

    df=df.withColumn("json_struct", from_json(col("json_string"), dynamic_schema))
    col_list=get_json_keys(dynamic_schema, "json_struct")
    
    print(df.select("id", *col_list))
    return df.select("id", *col_list)
"""

spark.stop()
