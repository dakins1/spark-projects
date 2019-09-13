
package sparkrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import org.apache.arrow.flatbuf.Bool
// import swiftvis2.spark._

case class StationRow(id:String, lat:Double, lon:Double, elev:Double, state:String, name:String)
case class ReportRow(id:String, year:Int, mmdd:Int, obType:String, obValue:Int)

object SparkRDD {
  def main(args:Array[String]) {

    //System.setProperty("hadoop.home.dir", "C:\\Users\\Dillon\\comp\\environmentVars\\winutils\\")

    val conf = new SparkConf().setAppName("SparkRDD")
      .setMaster("local[*]") //deactivate this when you're on the lab machines (or maybe not for this assignment)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //val stationLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\ghcnd-stations.txt")
    val stationLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
    val stationData = stationLines
      .map { line =>
        StationRow(
          line.slice(0,11),
          line.slice(12,20).toDouble,
          line.slice(21,30).toDouble,
          line.slice(31,37).toDouble,
          line.slice(38,40),
          line.slice(41,71)
        )
      }.cache()

    val reportLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
    val reportData = reportLines
      .map { line =>
        val p = line.split(",")
        ReportRow(
          p(0),
          p(1).take(4).toInt,
          p(1).takeRight(4).toInt,
          p(2), 
          p(3).toInt
        )
      }.cache()


    /* 1. Number of Stations in Texas */
    val txStations = stationData.filter(sr => sr.state == "TX")
    println(txStations.count)

    /* 2. Number of TX stations that reported data in 2017 */
    // val numInTX = reportData.filter(d => 

      
    sc.stop()
    
  }
}