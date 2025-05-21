
# Calculate % of mark for each students. Each subject is 100% marks.
# Create a result column by follwoing the below steps
#1. % Marks greater than or equal to 70 then 'Distinction' 
#2. % Marks greater than or equal to 60-69 then 'First Class' 
#3. % Marks greater than or equal to 50-59 then 'Second Class' 
#4. % Marks greater than or equal to 40-49 then 'Third Class' 
#4. % Marks greater than or equal to 40-49 then 'Fail' 

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a spark session
spark = SparkSession.builder.appName("exampel1").getOrCreate()

# Data for the dataframe creation
data1=[(1,"Steve"),(2,"David"),(3,"John"),(4,"Shree"),(5,"Helen")]
data2=[(1,"SQL",90),(1,"PySpark",100),(2,"SQL",70),(2,"PySpark",60),(3,"SQL",30),(3,"PySpark",20),(4,"SQL",50),(4,"PySpark",50),(5,"SQL",45),(5,"PySpark",45)]

schema1 = ["id","Name"]
schema2 = ["id","Subject","Mark"]

# Create Dataframe
df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)

# Try to disply both of the datafram 
#print(df1)
#df1.show(5)
# display(df1)
# Join the dataframe 
df_join = df1.join(df2, df1.id==df2.id).drop(df2.id)

# do the group by 
df_per = df_join.groupBy("id","Name").agg(sum("Mark")/count('*')).alias("Percentage")
df_per.show()