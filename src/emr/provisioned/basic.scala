// need to compile this as a jar and upload compiled jar to s3.
// Configure scala sbt project in intelliJIdea and put the jar there- no need to include dependencies, since its included on EMR

// Do not package standard framework libraries. Amazon EMR already includes standard big data runtimes based on the release version you select. 
// You should mark these as provided in your build tool (like Maven or SBT) so they aren't bundled into your JAR: Apache Spark Core and SQL libraries Apache Hadoop HDFS and YARN librariesAWS SDKs (such as the tools needed to read from/write to Amazon S3)

// aws emr add-steps --cluster-id j-03163421NSYHI3NU5X4V --region ap-south-1 --steps 'Type=CUSTOM_JAR,Name="Run spark-submit using command-runner.jar",ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[spark-submit,--deploy-mode,cluster,--class,basicSpark,s3://sreejits-bucket/spark-code/basicSparkScala-assembly-0.1.0-SNAPSHOT.jar]'

import org.apache.spark.sql.SparkSession

object basicSpark {

  val spark=SparkSession.builder.appName("basic").getOrCreate()

  val s3PathIp="s3://sreejits-bucket/employees/sample_recs.csv"

  val s3OpPath="s3://sreejits-bucket/employees_emr_op"


def main(args: Array[String])={

  val idf= basicSpark.spark.read.
    format("csv").
    option("path",basicSpark.s3PathIp).
    option("inferSchema",true).load()

  idf.write.format("parquet").option("path",basicSpark.s3OpPath).save()

}

  }