import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

object StreamClass {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .config("spark.sql.warehouse.dir", "C:\\Users\\Chinmay\\IdeaProjects\\Spark_Aurora_POC\\warehouse_dir")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val schema = StructType(
      Array(StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("salary", IntegerType),
        StructField("job", StringType)))

    //create stream from folder
    val fileStreamDf = sparkSession.readStream
      .option("header", "true")
      .option("sep",",")
      .schema(schema)
      .csv("C:\\Users\\Chinmay\\IdeaProjects\\Demo_SBT\\input")
    //    val inputdf = sparkSession.readStream.format("socket").option("host","localhost").option("port","8080").load()


    val query = fileStreamDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append()).start()
    //    val query = inputdf.writeStream.format("console").outputMode("append").start()

    query.awaitTermination(timeoutMs = 50000)
  }
}
