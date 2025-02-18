from pyspark.sql  import SparkSession
spark =SparkSession.builder.appName("using sql dataframe Transformations").getOrCreate()
df = spark.read.format('parquet').load("E:\\mtcars.parquet")
# df.show(5,True)
df.createOrReplaceTempView("emp_table")
# spark.sql("""select carb from emp_table group by carb having carb in(1,2,3,4,6) order by carb desc
# """).show()
#having carb in(1,2,3,4,6) order by carb desc
# df.createOrReplaceTempView("dep_table")

#using sql  (SELECT).........................................................
# spark.sql(""" select * from dep_table where carb between 1 and 1.5 limit 3
# """).show()
# update table dep_table set am = 100 where vs = 1
# select * from dep_table limit 5

#using dataframe
#df.select("disp","hp","vs").show(5,False)

#using sql
# spark.sql("""select disp,hp,vs from emp_table
# """).show(5,False)

#WHERE CLAUSE.........................................................
#
# spark.sql("""select * from emp_table where vs = 1
# """).show(6,True)

# df.where("vs = 1").show(6,True)

#GROUP BY CLAUSE......................................................

# spark.sql("""select carb,sum(cyl) from emp_table group by carb
# """).show()
#
# df.groupBy("carb").count().show()


#ORDER BY CLAUSE

spark.sql("""select carb,vs from emp_table order by carb DESC
""").show()
# df.orderBy("carb").show(5,True)




























