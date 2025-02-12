import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

sample_data = [("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Kumar", "Marketing", 2000),
              ("Saif", "Sales", 4100)
              ]
columns= ["employee_name", "department", "salary"]

df = spark.createDataFrame(sample_data,columns )

#
# df.select(first("salary")).show(truncate = False)
#
# df.select(last("salary")).show(truncate=False)
#
# df.select(kurtosis("salary")).show(truncate=False)
# df.select(max("salary")).show(truncate=False)
# df.select(min("salary")).show(truncate=False)
# df.select(mean("salary")).show(truncate=False)
# df.select(skewness("salary")).show(truncate=False)
df.select(sum("salary")).show(truncate=False)
df.select(sumDistinct("salary")).show(truncate=False)

