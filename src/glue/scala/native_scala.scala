//  write plain scala spark code in glue
// remeber to create a conf      --class and mention entry point  "com.apple.airflowapp"

package com.apple
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object airflowapp {
    
val co= new SparkConf()

  val spark= SparkSession.builder().appName("airflow").config(co).getOrCreate()

  def main(args: Array[String])={

    val df =spark.range(1,1000)

    val df1=df.select("id")
    df1.show()
    

  }
}
