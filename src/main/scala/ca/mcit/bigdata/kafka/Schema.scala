package ca.mcit.bigdata.kafka

case class Trip(tripId: String,
                serviceId: String,
                routeId: Int,
                tripHeadSign: String,
                wheelChairAccessible: Int)
object Trip {
  def apply(in: String): Trip = {
    val fields: Array[String] = in.split(",", -1)
    Trip(fields(2), fields(1), fields(0).toInt, fields(3),fields(6).toInt)
  }
}

case class EnrichedTrip(trip: Trip,
                        date: String,
                        exceptionType: Int,
                        routeLongName: String,
                        routeColor: String)
object EnrichedTrip {
  def toCsv(output: EnrichedTrip): String = {
    s"${output.trip.tripId}," +
      s"${output.trip.serviceId}," +
      s"${output.trip.routeId}," +
      s"${output.trip.tripHeadSign}," +
      s"${output.trip.wheelChairAccessible}," +
      s"${output.date}," +
      s"${output.exceptionType}," +
      s"${output.routeLongName}," +
      s"${output.routeColor}\n"
  }
}