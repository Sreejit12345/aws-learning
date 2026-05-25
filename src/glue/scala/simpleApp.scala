// https://docs.aws.amazon.com/glue/latest/dg/glue-etl-scala-example.html        get imports from here
// remeber to create a conf      --class and mention entry point  "simpleApp"

//  read from catalog in glue using glue apis


import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._


object simpleApp {
  def main(sysArgs: Array[String]) {
    val sparkctx: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sparkctx)
    val spark: SparkSession = glueContext.getSparkSession
    
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val df=spark.range(1,1000)
    df.show()
    
    
    val dframe1=glueContext.getCatalogSource(
            database ="dev-bronze-sales",
            tableName ="sourceb"
            ).getDynamicFrame()
    
    val df1=dframe1.toDF()
    
    df1.show()
        
            
    Job.commit()
  }
}