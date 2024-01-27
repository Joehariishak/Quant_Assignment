import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q2 {

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
     println("Question 2")
     println("======================================")
     println("Find the names of the 100 most frequent flyers.")
     println("")

      val flightsDS = flights
       .withColumnRenamed("passengerId", "passId")
       .withColumn("date", to_date(col("date"), "dd/MM/yyy"))
       //.show()

     val flightInfos = passengers
       .joinWith(flightsDS, flightsDS("passId") === passengers("passengerId"), "left")
       .map{
         case(p,f) => FlightInfo(p.passengerId, p.firstName, p.lastName, f.getInt(1), f.getString(2), f.getString(3), f.getDate(4))
       }

     val groupedFlights = flightInfos.groupBy(
         col("passengerId"),
         col("firstName"),
         col("lastName"))
       .agg( count("passengerId").as("count"))
       .sort(col("count").desc)
       .select(
         col("passengerId").as("Passenger Id"),
         col("count").as("Number of Flights"),
         col("firstName").as( "First Name"),
         col("lastName").as("Last Name"))
      .show()

  }
}
