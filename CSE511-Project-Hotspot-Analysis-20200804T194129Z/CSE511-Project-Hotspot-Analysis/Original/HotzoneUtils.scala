package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    if(queryRectangle == null || queryRectangle.isEmpty || pointString == null || pointString.isEmpty || queryRectangle.split(",").length < 4 || pointString.split(",").length < 2){
      return false
    }

    val Array(x1, y1, x2, y2) = queryRectangle.split(",").map(_.toDouble)
    val Array(x,y) = pointString.split(",").map(_.toDouble)
    x1 <= x && x <= x2 && y1 <= y && y <= y2
  }


}
