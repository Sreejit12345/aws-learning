''''
So, we are testing out shared utils and if we can write modular code- the goal is we want to import this module, in 
any glue job.

'''


from pyspark.sql import functions as F

def rename_add_metadata(df):

    df=df.withColumn('src',F.lit("wheel"))

    return df