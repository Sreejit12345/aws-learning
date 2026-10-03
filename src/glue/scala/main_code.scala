// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue.html
// https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging-enable.html#monitor-continuous-logging-script

package com.apple.managedIceberg
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.amazonaws.services.glue.log.GlueLogger

/*

As a pre-requisite we created a s3 table bucket with name "first-s3-bucket" and enabled glue integration

The "s3_rest_catalog" that we create and set as default catalog is pointed to the "s3tablescatalog".

So the s3 table bucket "first-s3-bucket" is placed as a sub-catalog under  "s3tablescatalog" .

create database if not exists s3_rest_catalog.manageds3tables. This actually created DB under path
 "s3_rest_catalog"."first-s3-bucket" . Howver in spark sql we dont need to mention the bucket name, as that is set in spark conf

to refer to tables use: "s3_rest_catalog".<dbName>.<tableName>

Remeber this uses a REST catalog to connect to s3Tables.

*/

object spark_obj {
    
    val conf= new SparkConf()
    
    conf.set("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2")
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.defaultCatalog", "s3_rest_catalog")
    conf.set("spark.sql.catalog.s3_rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.s3_rest_catalog.type", "rest")
    conf.set("spark.sql.catalog.s3_rest_catalog.uri", "https://s3tables.ap-south-1.amazonaws.com/iceberg")      // replace Region here
    conf.set("spark.sql.catalog.s3_rest_catalog.warehouse", "arn:aws:s3tables:ap-south-1:468094684048:bucket/first-s3-bucket")   // replace bucker name and 
    conf.set("spark.sql.catalog.s3_rest_catalog.rest.sigv4-enabled", "true")
    conf.set("spark.sql.catalog.s3_rest_catalog.rest.signing-name", "s3tables")
    conf.set("spark.sql.catalog.s3_rest_catalog.rest.signing-region", "ap-south-1")   // replace Region here
    conf.set("spark.sql.catalog.s3_rest_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.s3_rest_catalog.rest-metrics-reporting-enabled","false")
    
    
    val spark=SparkSession
    .builder
    .appName("managed")
    .config(conf)
    .getOrCreate()
    
}

object main_code {
    def main(args: Array[String]):Unit={
        
        val logger = new GlueLogger   // these logs may go to error even if job succeeds
        
        // note, how we dont put the s3 bucket name here
        // s3_rest_catalog is defined in the spark conf at the top.
        // s3_rest_catalog points to the s3table bucket name as catalog
        
        import spark_obj.spark.implicits._
        
        val df = spark_obj.spark.range(1,1000)
        
        val df1= df.toDF("id")

        
        spark_obj.spark.sql("create database if not exists s3_rest_catalog.manageds3tables")
        
        spark_obj.spark.sql("SHOW TABLES IN `s3_rest_catalog`.`manageds3tables`").show()
        
        val counter=spark_obj.spark.sql("SHOW TABLES IN `s3_rest_catalog`.`manageds3tables`")
        .filter("tableName== 'managedicebergexample'")
        .count()
        
    
        if(counter==0){
            logger.info("Table does not exists- creating table... ")
            
            recreateTableAndInsertData(df1,logger)
            
        }
        
        else{
            
            print("Table already present..... need to drop and recreate... ")
            
            logger.info("Table already present..... need to drop and recreate... ")
            
            // https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-delete.html
            
            spark_obj.spark.sql("""DROP TABLE s3_rest_catalog.manageds3tables.managedicebergexample PURGE""")   // due to aws bug this might now work- as a workaround drop from athena manually.
            
            print("dropped table")
            logger.info("dropped table")
            
            logger.info("Recreating table... ")
            
            recreateTableAndInsertData(df1,logger)
            
            
        }
    
        
    }
    
    def recreateTableAndInsertData(df:org.apache.spark.sql.DataFrame, logger: com.amazonaws.services.glue.log.GlueLogger):Unit={
        
        spark_obj.spark.sql("""CREATE TABLE s3_rest_catalog.manageds3tables.managedicebergexample(
             id bigint
             )
             USING iceberg
             TBLPROPERTIES (
              'format-version'='2'
            );
            
            """)
            
            print(" table created...")
            logger.info("Table created..... ")
            
            
            print("loading table with data")
            logger.info("Loading table with data ")
            
            df.writeTo("s3_rest_catalog.manageds3tables.managedicebergexample")
            .append()
        
        
    }
}


