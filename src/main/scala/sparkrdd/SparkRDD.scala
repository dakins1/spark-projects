
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
      .setMaster("local[*]") 
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val countriesLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-countries.txt")
    val countries = countriesLines.collect.foldLeft((scala.collection.immutable.Map.empty[String, String])){
      (map, line) => map + (line.take(2) -> line.drop(3))
    }

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

    /* 2. Number of TX stations that reported data in 2017 */
    val txStationSet = txStations.map(_.id).collect.toSet
    val numInTx = reportData.filter(d => txStationSet.contains(d.id)).map(_.id).distinct().count()

    /* 3. Highest temp reported anywhere this year. When and where was it? */
    val tMaxReport = reportData.filter(r => r.obType == "TMAX").sortBy(_.obValue).collect.takeRight(1)(0)
    val tMaxStation = stationData.filter(r => r.id == tMaxReport.id).take(1)(0)
    val res3 = tMaxReport.obValue/10.0 + " " + countries(tMaxStation.id.take(2)) + " " + tMaxStation.state + " " + tMaxReport.year + " " + tMaxReport.mmdd
    
    /* 4. How many stations haven't reported data in 2017? */
    val stationSet = stationData.map(_.id).collect().toSet
    val unreporters = reportData.filter(d => !stationSet.contains(d.id)).distinct().count()
    println(unreporters)

    /* 5. Max rainfall for any station in Texas during 2017, what station and when */
    val txMaxRainfall = reportData.filter(r => r.obType == "PRCP").sortBy(_.obValue).collect().takeRight(1)(0)

    //make sure to divide by 10


    sc.stop()
    
  }
}