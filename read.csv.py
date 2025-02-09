#importing sparksession

from pyspark.sql import SparkSession

#creating sparksession

spark = SparkSession.builder.appName("pyspark practice").getOrCreate()


#access spark context

spark_context = spark.sparkContext

#access sql context



#read a csv file

data_frame = spark.read.csv("C:/Users/hp/Downloads/MOCK_DATA.csv",header= True ,inferSchema=True)

data_frame.show(100,truncate = False)

