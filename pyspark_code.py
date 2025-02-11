from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("creating_dataframe").getOrCreate()

data = [(1,"gopi",1000),(2,"prasanth",2000),(3,"shiva",3000),(4,"sai kumar",4000)]

columns = ["emp_id","emp_name","emp_salary"]

df =spark.createDataFrame( data, columns )

df.show()