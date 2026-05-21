'''
This demonstrates how we can run native pyspark scripts in glue, and how we can reuse custom modules.

To reuse a custom module:

1) add setup.py in project root
2) make sure to do "pip install setuptools wheel" and  "pip install --upgrade setuptools wheel"
3) then run "python setup.py bdist_wheel" from the dir where setup.py is placed
4) step 3 should produce a dist folder, inside which a .whl file should be present
5) place that .whl file in s3, copy the s3 uri like "s3://orders123456sree/utils/my_sample_package-1.0.0-py3-none-any.whl"
and put it in "Python library path" under job settings.

Note: make sure to have __init__.py under each folder- so python considers it a package
'''



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.glue.glue_shared_module.shared_utils import *

spark=SparkSession.builder.appName('s').getOrCreate()

df=spark.range(100)

wn=Window.partitionBy('id').orderBy(F.col('id').desc())

f=df.withColumn('ww',F.row_number().over(wn))

x=rename_add_metadata(f)  #func used from custom module

x.show()