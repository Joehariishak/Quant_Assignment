import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Q3 {

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
            println("Question 3")
            println("======================================")
            println("Find the greatest number of countries a passenger has been in without being in the UK.")
            println("For example, if the countries a passenger was in were: ")
            println("UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.")
            println("")

            flights
              .where(col("from").notEqual("uk").and(col("to").notEqual("uk")))
              .groupBy(col("passengerId").as("Passenger ID"))
              .agg(count("passengerId").as("Longest Run"))
              .sort(col("Longest Run").desc)
              .show()
  }
}