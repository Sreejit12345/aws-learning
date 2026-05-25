//https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-jdbc-home.html
//https://docs.aws.amazon.com/glue/latest/dg/glue-etl-scala-apis-glue-gluecontext.html
// pre-req we have already setup a connection from the aws glue connection tab
// also make sure in the job details, we are using the connection.


import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import com.amazonaws.services.glue.util.JsonOptions


object readJdbc {
  def main(sysArgs: Array[String]) {
    val sparkctx: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkctx)
    val spark: SparkSession = glueContext.getSparkSession
    
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val connectionOptions = JsonOptions(Map(
    "connectionName" -> "Aurora connection",     // has been pre-configured.
    "useConnectionProperties" -> "true", 
     "dbtable" -> "Persons",
    //"user" -> user, 
    //"password" -> password, 
    //"sampleQuery" -> query 
    ))
    
    val dframe1=glueContext.getSource(
            connectionType ="mysql",
            connectionOptions =connectionOptions
            ).getDataFrame()
    
    dframe1.show()


    // standard read using jdbc

    val df = spark.read.format("jdbc")
    .option("url", "jdbc:mysql://database-1-instance-1.cf0qu4c0i6i8.ap-south-1.rds.amazonaws.com:3306/demo")
    .option("dbtable", "demo.Persons")
    .option("user", "admin")
    .option("password", "s2d3s2d3")
    .load()

    df.show()

        
    Job.commit()
  }
}