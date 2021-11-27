import org.apache.spark.sql.SparkSession
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.Files.copy
import java.nio.file.Paths.get
import java.sql.ResultSet

import org.apache.avro.SchemaBuilder.array
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.catalyst.ScalaReflection.universe.typeOf
import org.apache.spark.sql.types.{DataTypes, DoubleType, StructType, VarcharType}

import scala.math.Ordered.orderingToOrdered

object browser {

  case class url(id:Int,url:String,title:String,visited_count:Int,typed_count:Int,last_visited_time:Int,hidden:Int)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("browser")
      .getOrCreate()

    val source = "C:\\Users\\a178\\Downloads\\History"
    val target = "C:\\Users\\a178\\Downloads\\testing\\History"

    /*if (File(target).exists) {
    {
      File(target).delete()
    }
      */

      implicit def toPath (filename: String) = get(filename)
      copy (source, target, REPLACE_EXISTING)


    val schema=new StructType()
      .add("id",DataTypes.IntegerType)
      .add("url",DataTypes.StringType)
      .add("title",DataTypes.StringType)
      .add("visited_count",DataTypes.IntegerType)
      .add("typed_count",DataTypes.IntegerType)
      .add("last_visited_time",DataTypes.IntegerType)
      .add("hidden",DataTypes.IntegerType)


    val sqlContext = spark.sqlContext



    val loaded_db = spark.read.format("jdbc")
      .option("url","jdbc:sqlite:/C:\\Users\\a178\\Downloads\\testing\\History")
      .option("dbtable","downloads")
      .load().createOrReplaceTempView("t")

    val m = spark.sql("select * from t").distinct()
    m.printSchema()
  /*
    import java.text.DecimalFormat

    val df = new DecimalFormat("####.")
    m.foreach(f=>println(df.format(f)))

    *
   */

    println("sjsjs")

    val metaData = sqlContext.read
      .format("jdbc")
      .options(Map("url" -> "jdbc:sqlite:/C:\\Users\\a178\\Downloads\\testing\\History",
        "customSchema"->"id INT,url BLOB ,title BLOB ,visit_count INT,typed_count INT,last_visit_time INT,hidden INT",
        "dbtable" -> "(SELECT * FROM urls) AS t"))
      .load()

    metaData.printSchema()

    val myTableNames = metaData.select("url").distinct().toDF()
    myTableNames.printSchema()
    val x = myTableNames.withColumn("url", myTableNames("url").cast("String"))
    x.printSchema()

    //x.foreach(row => println(BigDecimal.apply(row.getString(0))))
//ERRROR here
    x.foreach(row => println(row(0)))
    println("optumhe")

    import spark.implicits._
   // x.map(f => String.valueOf(f)).transform("S")
    x.show()
    //x.map(f => java.math.BigDecimal.valueOf(f).toString)


   //val r =  myTableNames.map(f => f.asInstanceOf[DoubleType])




    //


    val lines = spark.readStream
      .format("za.co.absa.spark.jdbc.streaming.source.providers.JDBCStreamingSourceProviderV1")
      .options(Map("url" -> "jdbc:sqlite:/C:\\Users\\a178\\Downloads\\testing\\History",
        "dbtable" -> "sqlite_master",
      "offsetColumn"->"tbl_name"))
      .load()


    // Generate running word count
    val wordCounts = lines.groupBy("tbl_name").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
