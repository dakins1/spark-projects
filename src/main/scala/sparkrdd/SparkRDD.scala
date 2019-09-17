
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
    // val countriesLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\ghcnd-countries.txt")
    val countries = countriesLines.collect.foldLeft((scala.collection.immutable.Map.empty[String, String])){
      (map, line) => map + (line.take(2) -> line.drop(3))
    }

    val stationLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
    // val stationLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\ghcnd-stations.txt")
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
    // val reportLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\2017.csv")
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
      }

    val reporters = reportData.map(_.id).distinct().collect().toSet
    val tempReporters = reportData.filter(r => r.obType == "TMAX" || r.obType == "TMIN").map(_.id).collect().toSet
      
/*
    /* 1. Number of Stations in Texas */
    val txStations = stationData.filter(sr => sr.state == "TX")

    /* 2. Number of TX stations that reported data in 2017 */
    val txStationSet = txStations.map(_.id).collect.toSet
    val numInTx = reportData.filter(d => txStationSet.contains(d.id)).map(_.id).distinct().count()

    /* 3. Highest temp reported anywhere this year. When and where was it? */
    val tMaxReport = reportData.filter(r => r.obType == "TMAX").sortBy(_.obValue).collect.takeRight(1)(0)
    val tMaxStation = stationData.filter(r => r.id == tMaxReport.id).take(1)(0)
    val res3 = tMaxReport.obValue/10.0 + " " + countries(tMaxStation.id.take(2)) + " " + tMaxStation.state + " " + tMaxReport.year + " " + tMaxReport.mmdd
    println(res3 + "****Make sure you get the date****`")
    /* 4. How many stations haven't reported data in 2017? */
    val stationSet = stationData.map(_.id).collect().toSet
    val unreporters = reportData.filter(d => !stationSet.contains(d.id)).distinct().count()

    val unreports = stationData.filter(d => !reporters.contains(d.id)).count
    println(unreports + " unreporting stations")
    // println(unreporters + "I hope this isn't 0")

    /* 5. Max rainfall for any station in Texas during 2017, what station and when */
    val txMaxRainfall = reportData.filter(r => r.obType == "PRCP" && txStationSet.contains(r.id)).sortBy(_.obValue).collect().takeRight(1)(0)
    println(txMaxRainfall)


    /* 6. Max rainfall for any station in India, what station and when */
    val indiaStationSet = stationData.filter(s => s.id.take(2) == "IN").map(_.id).collect().toSet
    val indiaMaxRainfall = reportData.filter(r => r.obType == "PRCP" && indiaStationSet.contains(r.id)).sortBy(_.obValue).collect().takeRight(1)(0)
    println(indiaMaxRainfall)

*/

    /* 7. How many stations associated with San Antonio, TX? */
    // val saStations = stationData.filter(s => (s.lat <= 29.6008 && s.lat >= 29.2300) && (s.lon >= -98.7206 && s.lon <= -98.2357))
    val saStations = stationData.filter(s => s.state == "TX" && s.name.contains("SAN ANTONIO"))
    println("San Antonio stations: " + saStations.count())

    /* 8. How many SA stations reported temperature data in 2017? */
    
    val saReporters = saStations.filter(s => tempReporters.contains(s.id)).count()
    println("SA reporters: " + saReporters)

    /* 9. Largest increase in daily high temps in SA */
    val saSet = saStations.map(_.id).collect().toSet
    val saReports = reportData.filter(r => saSet.contains(r.id))
    val saTemps = saReports.filter(r => r.obType == "TMAX").collect()

    // val days = saTemps.groupBy(_.mmdd)?
    val days = saTemps.groupBy(_.id).map(t => (t._1, t._2.groupBy(_.mmdd).map(t => (t._1, t._2.map(_.obValue).max)))
    // days = (StationID, (Date, MaxTemp for that Station))
    val maxDays = days.
    val maxDays = days.map{case (k,v) => (k, v.maxBy(_.obValue))}.toSeq.sortBy(_._2.mmdd).map(_._2)

    val (maxDiff, _) = maxDays.foldLeft((0, maxDays(0))){
      case ((diff, oldRow), newRow) =>
        val d = newRow.obValue - oldRow.obValue
        if (d > diff) (d, newRow)
        else (diff, newRow)
    }

    println("Max diff: " + maxDiff/10.0 + "degress celcius")

    /*   
    Top 
    29.600833, -98.720639
    Bottom
    29.230055, -98.235731
*/
    sc.stop()
  }
}