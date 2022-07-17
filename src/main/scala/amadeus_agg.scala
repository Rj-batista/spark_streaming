import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
    .load("/Users/mamadian_djalo/Documents/ESGI/Spark_Core/Projet/spark_streaming/Data/day-by-day/*.csv")

  /**
   *
   */

  val schema =  new StructType()
    .add("id",StringType,false)
    .add("Aeroport_de_depart",StringType,false)
    .add("ville_de_depart",StringType,false)
    .add("date_de_depart",StringType,false)
    .add("aeroport_darrivee",StringType,false)
    .add("ville_darrivee",StringType,false)
    .add("date_darrivee",StringType,false)
    .add("Company",StringType,true)
    .add("price",DoubleType,false)
    .add("currency",StringType,false)
    .add("escale",StringType,true)
    .add("ville_descale",StringType,true)
    .add("aeroport_descale",StringType,true)
    .add("date_darrivee_escale",StringType,true)
    .add("date_depart_escale",StringType,true)
    .add("lastTicketingDate",StringType,false)


  amadeusDf.createOrReplaceTempView("amadeus_table")

  /**
   *
   */
  val amadeusStream = sparkSession
    .readStream
    .schema(schema)
    .format("csv")
    .option("maxFilesPerTrigger","1")
    .option("header","true")
    .load("/Users/mamadian_djalo/Documents/ESGI/Spark_Core/Projet/spark_streaming/Data/day-by-day/*.csv")

  println("spark is streaming " + amadeusStream.isStreaming)


// Streaming Action

  /*println("########################################")
  print("###################")
  println(" averagePriceByMonth ")
  println("###################")
  println("########################################")

  val averagePriceByMonth = amadeusStream.selectExpr(
    "id",
    "ville_de_depart",
    "ville_darrivee",
    "date_de_depart",
    "price"
  ).groupBy(
    window(col("date_de_depart"), "30 days") as "Date"
  ).avg("price")

  averagePriceByMonth.writeStream
    .format("memory")
    .queryName("averagePriceByMonth_table")
    .outputMode("complete")
    .start

  println(sparkSession.streams.active)
  for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |Select * from averagePriceByMonth_table
        |ORDER BY Date
        |""".stripMargin
    ).show(false)
    Thread.sleep(1000)
  }*/


  /*println("########################################")
  print("###################")
  print(" averagePriceByCompany ")
  println("###################")
  println("########################################")

  val averagePriceByCompany = amadeusStream.selectExpr(
    "Company",
    "price"
  ).groupBy(
    col("Company"),
  ).avg("price")

  averagePriceByCompany.writeStream
    .format("memory")
    .queryName("averagePriceByCompany_table")
    .outputMode("complete")
    .start

  println(sparkSession.streams.active)
  for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |Select * from averagePriceByCompany_table
        |ORDER BY Company
        |""".stripMargin
    ).show(false)
    Thread.sleep(1000)
  }*/


  /**
   *
   */

  /*println("########################################")
  print("###################")
  print(" nbTicketByCompany ")
  println("###################")
  println("########################################")
  val nbTicketByCompany = amadeusStream.selectExpr(
    "Company",
    "id"
  ).groupBy(
    col("id"),
    col("Company"),
  ).count()

  nbTicketByCompany.writeStream
    .format("memory")
    .queryName("nbTicketByCompany_table")
    .outputMode("complete")
    .start

  println(sparkSession.streams.active)
  for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |Select Company, count(*) from nbTicketByCompany_table
        |GROUP BY Company
        |ORDER BY count(*) DESC
        |""".stripMargin
    ).show(false)
    Thread.sleep(1000)
  }*/

  println("########################################")
  print("###################")
  print(" jourPlusCher ")
  println("###################")
  println("########################################")
  val jourPlusCher = amadeusStream.selectExpr(
    "date_de_depart", "price", "id"
  ).groupBy(
    col("date_de_depart"),
    col("price"),
    col("id")
  ).count()

  jourPlusCher.writeStream
    .format("memory")
    .queryName("nbTicketByCompany_table")
    .outputMode("complete")
    .start

  println(sparkSession.streams.active)
  for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |SELECT dayofweek(nbticketbycompany_table.date_de_depart), avg(price)
        |FROM nbTicketByCompany_table
        |GROUP BY dayofweek(nbticketbycompany_table.date_de_depart)
        |ORDER BY dayofweek(nbticketbycompany_table.date_de_depart)
        |""".stripMargin
    ).show(false)
    Thread.sleep(1000)
  }


}
