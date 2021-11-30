import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object jdbcStream {

  def readStreamJDBC(spark :SparkSession, userSchema :StructType,inputFolder: String): DataFrame={
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .option("header","true")
      .schema(userSchema)      // Specify schema of the csv files
      .csv(inputFolder+"\\")

    csvDF
  }

}
