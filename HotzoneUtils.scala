package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
  
  val rect_coordinates = queryRectangle.split(",")
  val point_coordinates = pointString.split(",")

  val lat_point: Double = point_coordinates(0).trim.toDouble
  val lon_point: Double = point_coordinates(1).trim.toDouble
  val lat1_rec: Double = math.min(rect_coordinates(0).trim.toDouble, rect_coordinates(2).trim.toDouble)
  val lat2_rec: Double = math.max(rect_coordinates(0).trim.toDouble, rect_coordinates(2).trim.toDouble)
  val lon1_rec: Double = math.min(rect_coordinates(1).trim.toDouble, rect_coordinates(3).trim.toDouble)
  val lon2_rec: Double = math.max(rect_coordinates(1).trim.toDouble, rect_coordinates(3).trim.toDouble)

  if ((lat_point >= lat1_rec) && (lon_point >= lon1_rec) && (lat_point <= lat2_rec) && (lon_point <= lon2_rec)) {
    return true
  }
  return false
  }
}
