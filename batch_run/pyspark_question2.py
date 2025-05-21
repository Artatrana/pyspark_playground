# how to get department wise nth highest salary?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Create a spark session
spark = SparkSession.builder.appName("exampel1").getOrCreate()

# Data for the dataframe creation
data1=[(1,"A",1000,"IT"),(2,"B",1500,"IT"),(3,"C",2500,"IT"),(4,"D",3000,"HR"),(5,"E",2000,"HR"),(6,"F",1000,"HR")
       ,(7,"G",4000,"Sales"),(8,"H",4000,"Sales"),(9,"I",1000,"Sales"),(10,"J",2000,"Sales")]

# Define the schema
schema1=["EmpId","EmpName","Salary","DeptName"]
#print(type(schema1))


# Create the dataframe
df = spark.createDataFrame(data1, schema1)

df_rank = df.select ('*', dense_rank().over(Window.partitionBy(df.DeptName).orderBy(df.Salary.desc())).alias('rank'))
df_rank.filter(df_rank.rank==2).show()




