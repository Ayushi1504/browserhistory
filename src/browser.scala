import org.apache.spark.sql.{Row, SparkSession}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy
import java.nio.file.Paths.get
import org.apache.spark.sql.functions.{col, collect_list, collect_set, desc, length, max, sum}
import org.apache.spark.sql.types.{DataTypes, DecimalType, DoubleType, StructType, VarcharType}


object browser {
/*
  case class url(id:Int,url:String,title:String,visit_count:Int,typed_count:Int,last_visited_time:Int,hidden:Int)
  def parse(f : Row): Row ={
    f.toString().split(",")
    val fields= lines.split(",")
    val id = fields(0).toFloat
    val cost = fields(2).toFloat
    (id,cost)
  }

   */



  def main(args: Array[String]): Unit = {

    val localGoogleChromeHistoryFilePath ="C:\\Users\\a178\\Downloads\\History"
    val workingInputDirectory = "C:\\Users\\a178\\IdeaProjects\\browserhistory\\resources\\input"
    val workingOutputDirectory = "C:\\Users\\a178\\IdeaProjects\\browserhistory\\resources\\output"

    analyzeYourChromeBrowserHistory(localGoogleChromeHistoryFilePath,workingInputDirectory,workingOutputDirectory)
  }

  def analyzeYourChromeBrowserHistory(localGoogleChromeHistoryFilePath :String,workingInputDirectory : String ,workingOutputDirectory : String): Unit ={

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("browser")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //this function will update the history table as a stream every 5 mins

    repeating_task_to_update_browsing_history(spark,localGoogleChromeHistoryFilePath,workingInputDirectory,workingOutputDirectory)

    //this is the schema for sqlite_master table
    /*
    val userSchema = new StructType()
      .add("type", "string")
      .add("name", "integer"
      .add("tbl_name","string")
      .add("rootpage","long")
      .add("sql","string")
     */

    // this is the schema for users table
    /*
    val schema=new StructType()
      .add("id",DataTypes.IntegerType)
      .add("url",DataTypes.StringType)
      .add("title",DataTypes.StringType)
      .add("visit_count",DataTypes.IntegerType)
      .add("typed_count",DataTypes.IntegerType)
      .add("last_visited_time",DataTypes.IntegerType)
      .add("hidden",DataTypes.IntegerType)
     */


    val userSchema = new StructType()
      .add("url", "string")
      .add("title", "string")
      .add("visit_count","integer")
      .add("typed_count","integer")
      .add("last_visit_time","string")
      .add("hidden","integer")
      .add("visit_time","string")
      .add("from_visit","integer")
      .add("transition","integer")

    //schema for the join query result

    //using defined jdbc streaming api

    import spark.implicits._
    val resultSet = jdbcStream.readStreamJDBC(spark,userSchema,workingOutputDirectory)

    val resultSet2 = resultSet
      .groupBy("title")
      .agg(sum("visit_count").as("visit_count"),
        sum("typed_count").as("typed_count"),
        collect_list("url").as("url"),
        collect_list("last_visit_time").as("last_visit_time"))
      .sort(desc("visit_count"),desc("typed_count"))

     // .select("url").map(f=>f.toString().split(",").maxBy(_.length))

    val query = resultSet2.writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows",40)
      .option("truncate",false)
      .start()

    query.awaitTermination()
  }

  def repeating_task_to_update_browsing_history(spark :SparkSession,localGoogleChromeHistoryFilePath :String,workingInputDirectory : String,workingOutputDirectory:String){

    println( "Updating Browser History File to JDBC Stream")

    val t = new java.util.Timer()
    val task = new java.util.TimerTask {

      def run(): Unit ={

        //Function to copy the history file in input location
        val source = localGoogleChromeHistoryFilePath
        val target = workingInputDirectory+"\\History"
        implicit def toPath (filename: String) = get(filename)
        copy (source, target, REPLACE_EXISTING)

        //load the table in csv format

        // Query needed to be grouped
        //SELECT urls.url, urls.title, urls.visit_count, urls.typed_count, urls.last_visit_time, urls.hidden, visits.visit_time, visits.from_visit, visits.transitionFROM urls, visitsWHERE urls.id = visits.url
        //This SQL statement extracts all the URLs the user visited alongside the visit count, type and timestamps.

        val jdbcUrl = "jdbc:sqlite:/"+target

        val visits_table = spark.read.format("jdbc")
          .option("url",   jdbcUrl)
          .option("dbtable"," ( select from_visit, transition, url, cast( visit_time as text ) from ( SELECT from_visit, transition, url, datetime( visit_time / 1000000 + (strftime('%s', '1601-01-01')), 'unixepoch', 'localtime') as visit_time FROM visits ) as visits )")
          .load()
          .withColumnRenamed("cast( visit_time as text )","visit_time")
          .createOrReplaceTempView("visits")

        //visits_table.printSchema()
        // visits_table.show()

        val urls_table = spark.read.format("jdbc")
          .option("url",  jdbcUrl)
          .option("dbtable","(select id , cast( url as text ),  cast( title as text ) , visit_count , typed_count , cast( last_visit_time as text ) , hidden from ( SELECT id, url, title, typed_count, visit_count, hidden, datetime( last_visit_time / 1000000 + (strftime('%s', '1601-01-01')), 'unixepoch', 'localtime') as last_visit_time FROM urls ) as urls )")
          .load()
          .withColumnRenamed("cast( title as text )","title")
          .withColumnRenamed("cast( url as text )","url")
          .withColumnRenamed("cast( last_visit_time as text )","last_visit_time")
          .createOrReplaceTempView("urls")

       // urls_table.printSchema()
       // urls_table.show()

        val join_result = spark.sql("SELECT urls.url, urls.title, urls.visit_count, urls.typed_count, urls.last_visit_time, urls.hidden, visits.visit_time , visits.from_visit, visits.transition FROM urls, visits WHERE urls.id = visits.url")
        //join_result.printSchema()
        //join_result.show()

       // join_result.select("visit_time").map(f=>datetime(f/1000000 + (strftime('%s', '1601-01-01')), 'unixepoch', 'localtime'))
        spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

        //save it in a csv format
        val outputfolder = workingOutputDirectory+"/myfolder/"

        val csv_format = join_result
          .coalesce(1)
          .write
          .format("om.databricks.spark.csv")
          .mode("overwrite")
          .option("header",true)
          .option("delimiter",",")
          .csv(outputfolder)

        Thread.sleep(100)

        import org.apache.hadoop.fs._
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val file = fs.globStatus(new Path(outputfolder+"part*"))(0).getPath().getName()
        fs.rename(new Path(outputfolder + file), new Path(outputfolder+"/mytable.csv"))
        fs.delete(new Path(outputfolder), true)
        fs.delete(new Path(workingOutputDirectory+"/.mytable.csv.crc"), true)

        println( "Updating Completed !")
      }
    }
    t.schedule(task, 1000,300000)
  }

}
