from pyspark.sql import SparkSession

spark =SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

data = [("James","Smith","M",3000),("Anna","Rose","F",4000),
        ('Robert','Williams','M',6200)]

columns = ["firstname","lastname","gender","salary"]

df = spark.createDataFrame( data,columns )



# Add new constanct column
from pyspark.sql.functions import lit

df.withColumn("bonus_percent", lit(0.3))
df.show()

#Add column from existing column
df.withColumn("bonus_amount", df.salary*0.3)
df.show()

#Add column by concatinating existing columns
from pyspark.sql.functions import concat_ws
df.withColumn("name", concat_ws(",",
                                "firstname",'lastname'))
df.show()


from pyspark.sql.functions import when
df.withColumn("grade",
   when((df.salary < 4000), lit("A"))
     .when((df.salary >= 4000) & (df.salary <= 5000), lit("B"))
     .otherwise(lit("C"))).show()