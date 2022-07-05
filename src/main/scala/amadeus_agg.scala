import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats


object amadeus_agg extends App{
  /**Definition
   *
   */
  val sparkSession = SparkSession.
    builder()
    .master("local[*]")
    .appName("First app Streaming")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
  import sparkSession.implicits._
  sparkSession.conf.set("spark.sql.shuffle.partitions","5")


  /**
   *
   */
  val amadeusDf = sparkSession
    .read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("<path>/*.csv")

  /**
   *
   */
  val amadeusSchema = amadeusDf.schema

  amadeusDf.createOrReplaceTempView("amadeus_table")
  /**
   *
   */
  val amadeusStream = sparkSession
    .readStream
    .schema(amadeusSchema)
    .format("csv")
    .option("maxFilesPerTrigger","1")
    .option("header","true")
    .load("<path>/*.csv")

  println("spark is streaming " + amadeus_stream.isStreaming)


  // Streaming Action
  val something = amadeusStream.selectExpr(
    "CustomerID",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")
    .groupBy(
      col("CustomerID"),
      window(col("InvoiceDate"), "1 hour") as "invoiceDate"
    ).sum("total_cost")

  something.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_table")  // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start

  println(sparkSession.streams.active)

  for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |Select * from customer_table
        |ORDER BY `sum(total_cost)` DESC
        |""".stripMargin
    ).show(false)
    Thread.sleep(1000)
  }


}
