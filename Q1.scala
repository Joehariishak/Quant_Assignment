import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q1 {

  //Data sources
  private val flightSrc = "C:\\flightData.csv"
  private val passengerSrc = "C:\\passengers.csv"

  // Start a local spark session
  private val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .config("spark.sql.crossJoin.enabled", "true")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  //Flight Dataset
  val flights = spark.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv(flightSrc)
    .as[Flight]

  //Passengers Dataset
  val passengers = spark.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv(passengerSrc)
    .as[Passenger]

  def main(args: Array[String]): Unit = {

        println("======================================")
        println("Question 1")
        println("======================================")
        println("Find the total number of flights for each month.")
        println("")

        flights.withColumn("date", to_date(column("date")))
          .withColumn("month", month(col("date")))
          .groupBy(col("month"))
          .count()
          .selectExpr("month as Month", "count as `Number of Flights`")
          .sort(col(colName = "month"))
          .show()
  }
}