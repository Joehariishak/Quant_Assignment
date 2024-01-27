case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)
case class Passenger(passengerId: Int, firstName: String, lastName: String)
case class FlightInfo(passengerId: Int, firstName: String, lastName: String, flightId: Int, from: String, to: String, date: java.sql.Date)

