#Spark UseCase Solution

from __future__ import print_function
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
from pyspark.sql.readwriter import DataFrameWriter

if __name__ == "__main__":
    warehouse_location = abspath('spark-warehouse')
    spark = SparkSession.builder.appName("TCS Assesment").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()

    spark.sql("create  table if not exists tcsques(videoid string,uploader string,erval int,category string,length int,view int,rating int,ratingnum int,comments int,related string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile")

    spark.sql("create  table if not exists test1(category string,num_videos int)  STORED AS ParquetFile")
    spark.sql("create  table if not exists test2(videoid string,rating int)  STORED AS ParquetFile")

    spark.sql("LOAD DATA INPATH '/home/tom/youtubedata.txt' OVERWRITE INTO TABLE tcsques")
    df = spark.sql("SELECT * from tcsques")
    df.write.mode('overwrite').format("parquet").save("hdfs://localhost/home/tom/sparkhive")
    a = df.select("category").groupBy("category").count().sort("count",ascending=False).limit(5)
    a.show()    
    b = df.select(("videoid"),("rating")).sort("rating",ascending=False).limit(10)
    b.show()
    a.write.insertInto("test1",overwrite = True)
    c = spark.sql("SELECT * from test1")
    b.write.insertInto("test2",overwrite = True)
    c.write.mode('overwrite').format("parquet").save("hdfs://localhost/home/tom/spark1")
    b.write.mode('overwrite').format("parquet").save("hdfs://localhost/home/tom/spark2")

    spark.stop()
