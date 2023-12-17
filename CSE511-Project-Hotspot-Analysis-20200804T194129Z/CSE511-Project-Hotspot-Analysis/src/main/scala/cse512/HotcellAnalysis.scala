package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)


  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickupInfo")  
  
  val sample_df = spark.sql("select x, y, z, count(*) as total_count from pickupInfo where x>=" + minX + " and x<=" + maxX + " and y>="+minY +" and y<="+maxY+" and z>="+minZ+" and z<=" +maxZ +" group by x,y,z").persist()
  sample_df.createOrReplaceTempView("sample_data")      
  
  val result = spark.sql("select sum(total_count) as total_val, sum(total_count * total_count) as total_sqr from sample_data").persist()
  
  val total_val = result.first().getLong(0).toDouble
  val total_sqr = result.first().getLong(1).toDouble  
  
  val mean = (total_val / numCells)
  val std_dev = Math.sqrt((total_sqr / numCells) - (mean * mean))   
  
  val adj_df = spark.sql("select S1.x as x , S1.y as y, S1.z as z, count(*) as numNeighbors, sum(S2.total_count) as sigma from sample_data as S1 inner join sample_data as S2 on ((abs(S1.x-S2.x) <= 1 and  abs(S1.y-S2.y) <= 1 and abs(S1.z-S2.z) <= 1)) group by S1.x, S1.y, S1.z").persist()
  
  adj_df.createOrReplaceTempView("adj_df")
  
  spark.udf.register("ST_ZScore",(mean: Double, std_dev:Double, numNeighbors: Int, sigma: Int, numCells:Int)=>((HotcellUtils.ST_ZScore(mean, std_dev, numNeighbors, sigma, numCells))))  
  
  val GScore_df =  spark.sql("select x,y,z, ST_ZScore("+ mean + ","+ std_dev +",numNeighbors,sigma," + numCells+") as ZScore from adj_df")
  GScore_df.createOrReplaceTempView("GScore_df")
  
  val retDf = spark.sql("select x,y,z from GScore_df order by ZScore desc")
  return retDf // YOU NEED TO CHANGE THIS PART
}
}
