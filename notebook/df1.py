# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
column = ["ID", "Name"]
data = [("1", "zafar"),("2","imam"),("3","md")]
rdd = spark.sparkContext.parallelize(data)
dfFromRDD = rdd.toDF(column)
#display(dfFromRDD)
dfFromRDD.printSchema()
dfFromRDD1 = spark.createDataFrame(rdd).toDF(*column)
#display(dfFromRDD1)

#Create a DataFrame from List Collection in Databricks
#Using createDataFrame() from the SparkSession in Databricks
dfFromData = spark.createDataFrame(data).toDF(*column)
#display(dfFromData)

#Using createDataFrame() with the Row type in Databricks
rowData = map(lambda x: Row(*x), data)
dfFromData2 = spark.createDataFrame(rowData,column)
#display(dfFromData2)

#Create DataFrame with the schema in Databricks
data2 = [("James","","Smith","36636","M"),
    ("Michael","Rose","","40288","M"),
    ("Robert","","Williams","42114","M"),
    ("Maria","Anne","Jones","39192","F"),
    ("Jen","Mary","Brown","","F")
  ]
schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
  ])
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)
#Creating DataFrame from a CSV in Databricks
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/mdzaimam@in.ibm.com/Log.csv")
# drop rows with missing values
df1.dropna()
df1.take(10)
df1.show()
df2 = spark.read.csv("dbfs:/FileStore/shared_uploads/mdzaimam@in.ibm.com/Log.csv")
#df2.show()

# COMMAND ----------

