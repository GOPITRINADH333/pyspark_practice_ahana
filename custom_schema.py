from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id",StringType(), False)
])
spark = SparkSession.builder.master("local[1]").appName("custom_schema").getOrCreate()

data = [("1",),("2",),("3",)]

columns = ["id"]

df = spark.createDataFrame(data,columns)

df.show()

df.printSchema()

