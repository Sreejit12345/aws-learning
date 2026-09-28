// https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-iceberg-use-spark-cluster.html
// // need to compile this as a jar and upload compiled jar to s3.
// This is not using the cluster properties so we need to mention the below JAR, to make sure iceberg specific jar are imported in cluster
//--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar

// sample spark submit

// spark-submit --deploy-mode client --class nonManagedIceberg --conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar s3://sreejits-bucket/spark-code/basicSparkScala-assembly-0.1.0-SNAPSHOT.jar

// automatically catalogs table in glue catalog

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, rand}

object nonManagedIceberg extends App {


  val conf = new SparkConf()

  conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  conf.set("spark.sql.catalog.emr_catalog", "org.apache.iceberg.spark.SparkCatalog")
  conf.set("spark.sql.catalog.emr_catalog.warehouse", "s3://iceberg-demo123/warehouse/")
  conf.set("spark.sql.catalog.emr_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
  conf.set("spark.sql.catalog.emr_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO")

  val spark = SparkSession.builder().appName("iceberg").config(conf).getOrCreate()


  def generateDf():org.apache.spark.sql.DataFrame={

    var randomVal = scala.util.Random.nextInt(100)

    if(randomVal<=0){

      randomVal=1

    }


    val df =spark.range(1,randomVal)

    df.withColumn("id",col("id").cast("int"))
      .withColumn("isUpdated",lit("NO"))

  }

  spark.sql("create database if not exists emr_catalog.emriceberg").show(false)

  val all_tables = spark.sql("show tables in emr_catalog.emriceberg").collect()

  var table_exists = false

  for (i <- all_tables) {
    if ((i.getAs[String]("namespace") == "emriceberg") && (i.getAs[String]("tableName") == "emrexample")) {
      table_exists = true
    }
  }

  if(table_exists==false){  // first load

    spark.sql(
      """
        CREATE TABLE emr_catalog.emriceberg.emrexample
         (
          id INT,
          isUpdated STRING
         )
         USING ICEBERG
      """
    )

    val finalDf=generateDf()

    finalDf.writeTo("emr_catalog.emriceberg.emrexample").append()


  }

    else{   // merge

      val finalDf=generateDf()

      finalDf.createOrReplaceTempView("src")

      spark.sql(
        """
           MERGE INTO emr_catalog.emriceberg.emrexample t
           USING src s
           on(t.id=s.id)
           WHEN MATCHED THEN UPDATE SET t.isUpdated='YES'
           WHEN NOT MATCHED THEN INSERT *

         """
      )

    }


  }




