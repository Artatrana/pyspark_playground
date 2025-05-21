# how to get department wise nth highest salary?

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Create a spark session
spark = SparkSession.builder.appName("exampel1").getOrCreate()

# Data for the dataframe creation
data_salary=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),(200,"Joanne",100,1,"13-04-23",4500),(200,"Joanne",100,1,"14-04-23",4020)]


schema_salary=["EmpId","EmpName","Mgrid","deptid","salarydt","salary"]

df_salary = spark.createDataFrame(data_salary,schema_salary)

#department dataframe
data_department=[(1,"IT"), (2,"HR")]
schema_department=["deptid","deptname"]

df_department = spark.createDataFrame(data_department,schema_department)

df = df_salary.withColumn('Newsaldt', to_date('salary','dd-MM-yy'))

df_join = df.join(df_department, ['deptid'])

df_join2 = df_join.alias('a').join(df_join.alias('b'), col('a.MgrId')==col('b.EmpID'),'left')\
       .select(col('a.deptname'),
    col('b.EmpName').alias('ManagerName'),
    col('a.EmpName'),
    col('a.Newsaldt'),
    col('a.salary'))
#df_join2.show()
df_result = df_join2.groupBy('deptname','ManagerName','EMpName',year('Newsaldt').alias('Year')\
                             ,date_format('Newsaldt','MMMM').alias('Month')).sum('salary')

df_result.show()

 

