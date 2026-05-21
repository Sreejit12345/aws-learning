from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.glue.glue_shared_module.shared_utils import *

spark=SparkSession.builder.appName('s').getOrCreate()

df=spark.range(100)

wn=Window.partitionBy('id').orderBy(F.col('id').desc())

f=df.withColumn('ww',F.row_number().over(wn))

x=rename_add_metadata(f)

x.show()