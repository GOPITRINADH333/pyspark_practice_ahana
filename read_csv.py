from pyspark.sql import SparkSession
from pyspark.sql.functions import column
from pyspark.sql.functions import col

#creating sparksession
spark = SparkSession.builder.appName("pyspark practice").getOrCreate()

#access spark context
spark_context = spark.sparkContext

#read a csv file
data_frame = spark.read.csv("C:/Users/hp/Downloads/MOCK_DATA.csv",header= True ,inferSchema=True)
#hmoppett0@behance
#data_frame.show(5,truncate = False , vertical=True)
rename_column_df=data_frame.toDF("column 1","column 2","column 3","column 4","column 5","column 6")
rename_column_df.show()


sort_dataframe = rename_column_df.sort(col("column 1"),col("column 2"),col("column 3"),
                                       col("column 4"),col("column 5").desc())
sort_dataframe.show()

sort_dataframe.printSchema()











