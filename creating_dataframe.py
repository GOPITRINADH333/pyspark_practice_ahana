from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Broadcast_join").getOrCreate()

sample_data = [(1,"gopi",1000),(2,"ayyappa",2000),(3,"mahesh",3000),(4,"ravi",4000),(5,"venkatesh",5000)]

columns = ["emp_id","emp_name","salary"]

df = spark.createDataFrame(data = sample_data,schema =columns)

df.show()
