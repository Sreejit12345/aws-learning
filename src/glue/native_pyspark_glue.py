from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark=SparkSession.builder.appName('s').getOrCreate()  #this gets the already created session

df=spark.range(100)

wn=Window.partitionBy('id').orderBy(F.col('id').desc())

f=df.withColumn('ww',F.row_number().over(wn))


f.show()