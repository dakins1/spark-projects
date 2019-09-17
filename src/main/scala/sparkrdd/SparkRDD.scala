
package sparkrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle
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

    // val countriesLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-countries.txt")
    val countriesLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\ghcnd-countries.txt")
    val countries = countriesLines.collect.foldLeft((scala.collection.immutable.Map.empty[String, String])){
      (map, line) => map + (line.take(2) -> line.drop(3))
    }

    // val stationLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/ghcnd-stations.txt")
    val stationLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\ghcnd-stations.txt")
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

    // val reportLines = sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv")
    val reportLines = sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\2017.csv")
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
    
    val saReporters = saStations.filter(s => tempReporters.contains(s.id)).map(_.id).collect().toSet
    println("SA reporters: " + saReporters.size)

    /* 9. Largest increase in daily high temps in SA */
    val saSet = saStations.map(_.id).collect().toSet
    val saReports = reportData.filter(r => saSet.contains(r.id))
    val saTemps = saReports.filter(r => r.obType == "TMAX").collect()

    /*
    val days = saTemps.groupBy(_.id).map(t => (t._1, t._2.groupBy(_.mmdd).map(t => (t._1, t._2.map(_.obValue).max))))
    val maxDays = days.map(t => (t._1, t._2.toSeq.sortBy(_._1).map(_._2)))
    
    val maxDiffs = maxDays.map{
      case (id, reps) =>
        val (_, maxDiff) = reps.foldLeft((reps(0), 0)){
          case ((oldTemp, diff), newTemp) =>
            val newDiff = newTemp - oldTemp
            if (newDiff > diff) (newTemp, newDiff)
            else (newTemp, diff)
        }
        (id, maxDiff)
    }
    // maxDiffs.foreach(println)
    val maxDiff = maxDiffs.maxBy(_._2)
    println("Max difference: " + maxDiff)
    
    val days = saTemps.groupBy(_.id).map(t => (t._1, t._2.groupBy(_.mmdd).map(t => (t._1, t._2.map(_.obValue).max))))
    
    val saTmaxs = saReports.filter(s => s.obType == "TMAX").groupBy(_.id).map(t => (t._1 -> t._2.toSeq.sortBy(_.mmdd))).collect()
    val saPrcps = saReports.filter(s=> s.obType == "PRCP" && saReporters.contains(s.id)).groupBy(_.id).map(t => (t._1 -> t._2.toSeq.sortBy(_.mmdd))).collect()

    val pairs = saTmaxs.map{
      case (id, seq) =>
       val matchingPrcps = saPrcps.filter(_._1 == id).map(_._2).take(1)(0)
       val matches = seq.map(r => (r.obValue -> { 
         val x = matchingPrcps.filter(_.mmdd == r.mmdd)
         if (x.length == 1) Some(x(0).obValue)
         else None
        }))
       id -> matches.filter(_._2 != None).map(t => (t._1 -> t._2.get))
    }
    //first is temp, second is precipitation
    val coefs = pairs.map{
      case (id, ps) => 
        val tAvg = ps.map(_._1).sum / ps.length
        val pAvg = ps.map(_._2).sum / ps.length
        val numerator = ps.foldLeft(0){case (s, (t, p)) => s + (t - tAvg)*(p - pAvg)} 
        val denomLeft = scala.math.sqrt(ps.foldLeft(0.0){case (s, (t, _)) => s + scala.math.pow(t - tAvg, 2)})
        val denomRight = scala.math.sqrt(ps.foldLeft(0.0){case (s, (_, p)) => s + scala.math.pow(p - pAvg, 2)})
        (numerator) / (denomLeft * denomRight)
    }
    println(coefs.sum / coefs.length)

    */
    val ids = Set("CA005062040", "USC00081306", "USC00364778", "MGM00044287", "SPE00156540"  )
    val colorMap = Map("CA005062040" -> GreenARGB, "USC00081306" -> RedARGB, "USC00364778" -> BlackARGB, "MGM00044287" -> BlueARGB, "SPE00156540" -> YellowARGB)
    val specialReports = reportData.filter(r => r.obType == "TMAX" && ids.contains(r.id)).groupBy(_.id).map(s => s._1 -> s._2.toArray).collect()

    val grossMap = Map(1->0, 2->31, 3->59, 4->90, 5->120, 6->151, 7->181, 8->212, 9->243, 10->273, 11->304, 12->334)
    def convert(mmdd:Int) :Int = {
      val mm = if (mmdd > 999) mmdd/1000 else mmdd/100
      val dd = mmdd.toString.takeRight(2).toInt
      grossMap(mm) + dd 
    }

    /*
    val reporting = reportData.filter(s => s.obType == "TMAX")
    reporting.foreach(println)
    */
    

    val plots = specialReports.map{
      case (id, rows) => 
      val sorted = rows.sortBy(_.mmdd)
      ScatterStyle(sorted.map(r => convert(r.mmdd)), sorted.map(_.obValue/10), symbolWidth = 6, symbolHeight = 6, colors = colorMap(id))
    }.toSeq

    val tempPlot = Plot.stacked(plots, "Green is Canada, Red is Florida, Black is Pennsylvania, Blue is Mongolia, Yellow is Spain", "Day", "TMAX Celsius")
        SwingRenderer(tempPlot, 800, 800, true)

    // styles: Seq[PlotStyle], title: String, xLabel: String, yLabel: String, xType: Axis.ScaleStyle.Value, yType: Axis.ScaleStyle.Value): Plot
    sc.stop()
  }
}