from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD Transformations").getOrCreate()

#Transformations
#upper_case..........
# my_list = ["gopi","prasanth","shiva","jithendra","koti"]
# rdd = spark.sparkContext.parallelize(my_list)
# print(rdd.collect())
# new_rdd = rdd.map(lambda x: x.upper())
# print(new_rdd.collect())

#upper_case..................................................
# list = ["jyothi","prasanna","varalaxmi","sneha"]
# rdd = spark.sparkContext.parallelize(list)
# print(rdd.collect())
# new_rdd = rdd.map(lambda x:x.upper())
# print(new_rdd.collect())

#lower_case...................................................
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("creating rdd").getOrCreate()
# list = ["HARESH","MAHESH","SOMESH","JAGADEESH","RUPESH"]
# rdd = spark.sparkContext.parallelize(list)
# print(rdd.collect())
# new_rdd = rdd.map(lambda x : x.lower())
# print(new_rdd.collect())

#flat_map.....................................................
# list =[["1"],["2"],["3"],["4"],["5"]]
# rdd =spark.sparkContext.parallelize(list)
# print(rdd.collect())
# fn_rdd = rdd.flatMap(lambda x : x)
# print(fn_rdd.collect())

#flat_map.............................................................
# my_list = [["gopi trinadh"],["mahesh babu"],["mohan babu"],["bhargav kumar"]]
# rdd = spark.sparkContext.parallelize(my_list)
# #
# # print(rdd.collect())
# # fn_rdd = rdd.flatMap(lambda x:x)
# # print(fn_rdd.collect())

# fn_rdd = rdd.flatMap(lambda x:x.split(" "))
# print(fn_rdd.collect())
#print(rdd.collect())

#filter.........................................................
# new_rdd = rdd.map(lambda x : x * 10)
# print(new_rdd.collect())
# new_rdd = rdd.filter(lambda x : x >2)
# print(new_rdd.collect())
# new_rdd = rdd.filter(lambda x : x<2)
# print(new_rdd.collect())

#group_by_key................................................
# list = [(1,50),(2,50),(2,100),(1,60),(3,40),(3,80)]
# rdd = spark.sparkContext.parallelize(list)
# grouped_rdd = rdd.groupBykey()
# print(in grouped_rdd.collect())
# grouped_rdd1 = grouped_rdd.mapValues(lambda x : sum(x))
# print(grouped_rdd1.collect())

#group_by_key................................................
# list = [(1,50),(2,50),(2,100),(1,60),(3,40),(3,80)]
# rdd = spark.sparkContext.parallelize(list)
# grouped_rdd = rdd.groupByKey()
# print(grouped_rdd.collect())
# grouped_rdd1 = grouped_rdd.mapValues(lambda x: sum(x))
# print(grouped_rdd1.collect())

#group_by_key...................................................
#list = [(2,500),(5,600),(2,100),(5,100),(1,600)]
# rdd = spark.sparkContext.parallelize(list)
# grouped_rdd = rdd.groupByKey()
# print(grouped_rdd.collect())
# grouped_rdd1 =grouped_rdd.mapValues(lambda x:sum(x))
# print(grouped_rdd1.collect())

#reduced_by_key...............................................
# list = [(2,500),(5,600),(2,100),(5,100),(1,600)]
# rdd = spark.sparkContext.parallelize(list)
# print(rdd.collect())
# reduced_rdd =rdd.reduceByKey(lambda x,y : x+y)
# print(reduced_rdd.collect())

#reduced_by_key.........................................
# list = [(1,100),(2,200),(1,200),(2,100)]
# rdd =spark.sparkContext.parallelize(list)
# print(rdd.collect())
# reduced_rdd =rdd.reduceByKey(lambda x,y :x+y)
# print(reduced_rdd.collect())

#joins.............................method_1

# list1 =[(1,"gopi"),(2,"bhargav"),(3,"shiva"),(4,"jithu")]
# rdd1 =spark.sparkContext.parallelize(list1)
# list2 = [(1,"amaravathi"),(2,"ypalem"),(3,"mahaboob nagar"),(4,"pnnur")]
# rdd2 =spark.sparkContext.parallelize(list2)
# join_rdd =rdd1.join(rdd2)
# print(join_rdd.collect())

#join...............................method_2
# rdd1 =spark.sparkContext.parallelize([(1,"GOPI"),(2,"BHARGAV"),(3,"mahaboob nagar")])
# rdd2 =spark.sparkContext.parallelize([(1,"amaravathi"),(2,"ypalem")])
# joined_rdd =rdd1.join(rdd2)
# print(joined_rdd.collect())

#Distinct......................................
# list =([1,2,3,4,5,6,1,2,3,4,5,6])
# rdd =spark.sparkContext.parallelize(list)
# distinct_rdd =rdd.distinct()
# print(distinct_rdd.collect())

#Sortby.............................................
# list =([1,2,3,4,5,6,1,2,3,4,5,6])
# rdd =spark.sparkContext.parallelize(list)
# print(rdd.getNumPartitions())
# sort_by =rdd.sortBy(lambda x:x,ascending=True)
# print(sort_by.collect())

#union.................................................
# list1=([1,2,3])
# rdd1 =spark.sparkContext.parallelize(list1)
# list2=([4,5,6])
# rdd2=spark.sparkContext.parallelize(list2)
# union_rdd =rdd1.union(rdd2)
# print(union_rdd.collect())

#cartisian_join...........................................
# rdd1=spark.sparkContext.parallelize([1,2,3])
# print(rdd1.getNumPartitions())
# rdd2=spark.sparkContext.parallelize([4,5,6])
# cartisian_rdd =rdd1.cartesian(rdd2)
# print(cartisian_rdd.collect())

# Repartition..............................................
# rdd=spark.sparkContext.parallelize([1,2,3,4,5])
# print(rdd.getNumPartitions())
# #increase
# new_rdd =rdd.repartition(10)
# print(new_rdd.getNumPartitions())
# #decrease
# new_rdd =rdd.repartition(2)
# print(new_rdd.getNumPartitions())

# Coalesce...............................................
# rdd =spark.sparkContext.parallelize([1,2,3,4,5,6,7,8])
# print(rdd.getNumPartitions())
# new_rdd =rdd.coalesce(2)
# print(new_rdd.getNumPartitions())
# news_rdd =rdd.coalesce(8)
# print(news_rdd.getNumPartitions())

#word_count.................................................................
# rdd =spark.sparkContext.parallelize(" gopi is a gopi is a gopi is a good boy good boy")
# print(rdd.collect())
#
# new_rdd=rdd.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x,y :x+y)
# print(new_rdd.collect())


# rdd =spark.sparkContext.parallelize(["gopi","hareesh","ramesh","suresh","gopi","hareesh","ramesh","suresh"],4)
# print(rdd.collect())


# rdd = spark.sparkContext.parallelize(["gopi", "haresh"], 3)
# print(rdd.collect())
#
#
# rdd = spark.sparkContext.parallelize(["gopi"] * 3 + ["haresh"] * 5, 3)
# print(rdd.collect())


#ACTIONS...............................................................................


