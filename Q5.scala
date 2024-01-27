import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q5 {
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
    println("Question 5, Extra Marks")
    println("======================================")
    println("Find the passengers who have been on more than N flights together within the range (from,to).")
    println("")

    //def flownTogether(atLeastNTimes: Int, from: Date, to: Date) = {

    val flightsLeft = flights
      .withColumnRenamed("passengerId", "passengerId1")
      .withColumnRenamed("flightId", "flightId1")
      .withColumnRenamed("from", "from1")
      .withColumnRenamed("to", "to1")
      .withColumn("date1", to_timestamp(col("date"), "HH:mm:ss.SSS"))

    val flightsRight = flights
      .withColumnRenamed("passengerId", "passengerId2")
      .withColumnRenamed("flightId", "flightId2")
      .withColumnRenamed("from", "from2")
      .withColumnRenamed("to", "to2")
      .withColumn("date2", to_timestamp(col("date"), "HH:mm:ss.SSS"))

    flightsLeft
      .join(
        flightsRight,
        flightsLeft("flightId1") === flightsRight("flightId2")
      )
      .groupBy(col("passengerId1"), col("passengerId2"))
      .agg(count(col("flightId1")).as("count"),
        min(col("date1")).as("From"),
        max(col("date1")).as("To"))
      .where(
        col("passengerId1").notEqual(col("passengerId2")))
           .sort(col("count").desc)
      .select(
        col("passengerId1").as("Passenger 1 ID"),
        col("passengerId2").as("Passenger 2 ID"),
        col("count").as("Number of Flights Together"),
        col("From"),
        col("To")
        )
      .show()
  }
}