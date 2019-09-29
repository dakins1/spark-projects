package sparkrdd2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle
import org.apache.arrow.flatbuf.Bool
import org.apache.spark.rdd.RDD
import customML.BasicStats

// import swiftvis2.spark._

case class StationRow(id:String, lat:Double, lon:Double, elev:Double, state:String, name:String)
case class ReportRow(id:String, year:Int, mmdd:Int, obType:String, obValue:Int, qflag:String)

object SpecialRDD  {

def main(args:Array[String]):Unit = {    
val conf = new SparkConf().setAppName("Special RDD").setMaster("local[*]") //("spark://pandora00:7077")
  val sc = new SparkContext(conf)
  
  sc.setLogLevel("WARN")
  
    def parseReports(lines:RDD[String]): RDD[ReportRow] = {
      lines 
      .map { line =>
        val p = line.split(",")
        ReportRow(
          p(0),
          p(1).take(4).toInt,
          p(1).takeRight(4).toInt,
          p(2), 
          p(3).toInt,
          p(5)
        )
      }.filter(r => r.qflag == "")
    }
   
    // val reports2017 = parseReports(sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv"))
    val reports2017 = parseReports(sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\2017.csv"))
    val reports1897 = parseReports(sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\1897.csv"))

    // val reports1897 = parseReports(sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/1897.csv"))
    
    
    /*
    /* 1. Station with largest TMAX-TMIN in one day in 2017 */
    val reportPairs = reports2017.map(r => (r.id -> r.mmdd) -> r)
    val tmaxs = reportPairs.filter(p => p._2.obType == "TMAX")
    val tmins = reportPairs.filter(p => p._2.obType == "TMIN")
    val tPairs = tmaxs.join(tmins)
    val tDiffs = tPairs.map(p => p._1 -> (p._2._1.obValue - p._2._2.obValue))
    
    val maxReport2 = tDiffs.fold(tDiffs.first()){
      case ((key, diff), newPair) =>
        if (diff > newPair._2) (key, diff)
        else newPair
    }

    val d = tPairs.filter(r => r._1 == ("USC00502963" -> 213)).first()
    // println("Max: " + d._2._1 + " min: " + d._2._2)
    // println("Max difference in one day " + maxReport2)

    /* 2. Station with biggest tmax - tmin difference over the whole year */
    val reportsByStation = reports2017.map(r => r.id -> r)
    val maxReports = reportsByStation.filter(r => r._2.obType == "TMAX")
    val minReports = reportsByStation.filter(r => r._2.obType == "TMIN")
    val dummyMax = ReportRow("", 0, 0, "", scala.Int.MinValue, "")
    val maxMaxReports = maxReports.foldByKey(dummyMax){
      (rep, newRep) =>
        if (rep.obValue > newRep.obValue) rep
        else newRep
    }
    val dummyMin = ReportRow("", 0, 0, "", scala.Int.MaxValue, "")
    val minMinReports = minReports.foldByKey(dummyMin) {
      (rep, newRep) =>
      if (newRep.obValue > rep.obValue) rep
      else newRep
    }
    val joinedReps = maxMaxReports.join(minMinReports) 
    val yearDiffs = joinedReps.map{case (k, v) => k -> (v._1.obValue - v._2.obValue)}
    val maxYearDiff = yearDiffs.fold(yearDiffs.first())((old, nw) => if (old._2 > nw._2) old else nw)
    println("Max diff over year: " + maxYearDiff)
    println(joinedReps.filter(r => r._1 == "RSM00024585").first())
    
    /* 3. Std. Dev. for all Tmax's for each station */
    val usMaxs = reports2017.filter(r => r.id.take(2) == "US" && r.obType == "TMAX").map(_.obValue/10.0)
    val stdDevMax = customML.BasicStats.stdev(usMaxs)
    val usMins = reports2017.filter(r => r.id.take(2) == "US" && r.obType == "TMIN").map(_.obValue/10.0).collect().toSeq
    val stdDevMin = customML.BasicStats.stdev(usMins)
    println("StdDev max: " + stdDevMax + " min: " + stdDevMin)
    
    /* How many stations reported data in both 1897 and 2017? */
    // val reports1897 = parseReports(sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/1897.csv"))
    val pairs1897 = reports1897.map(r => r.id).distinct
    val pairs2017 = reports2017.map(r => r.id).distinct
    println("1897 and 2017 reporters: " + pairs2017.intersection(pairs1897).count()) //.map(_._1).distinct().count())
    */

    /* 5. Variability of temps with latitude */
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
    val stationPairs = stationData.map(s => s.id -> s)

    val rPairs = reports2017.map(r => r.id -> r).filter(p => p._1.take(2) == "US")
    val sar = stationPairs.join(rPairs)
    val latGroups = sar.map(p => p._2._1.lat -> p._2._2)
    val lat35 = latGroups.filter(_._1 < 35)
    val lat35and45 = latGroups.filter(p => 35 < p._1 && p._1 < 42)
    val lat45 = latGroups.filter(_._1 > 45)
    val latSeq = Seq(lat35, lat35and45, lat45)
    //make all the pairs first, then later you can filter it all by the desired obType

    /*
    val tmaxsDoubs = latSeq.map(_.filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble)).map(_.popStdev)
    tmaxsDoubs.foreach(println)
    val tmins = latSeq.map(_.filter(p => p._2.obType == "TMIN").map(p => (p._2.id, p._2.mmdd) -> p._2))
    val tmaxs = latSeq.map(_.filter(p => p._2.obType == "TMAX").map(p => (p._2.id, p._2.mmdd) -> p._2))
    val tMatches = tmins.zip(tmaxs).map(p => p._1.join(p._2).map(p1 => ((p1._2._2.obValue + p1._2._1.obValue.toDouble)/2)))
    val taveDoubles = tMatches.map(_.popStdev()/10.0)
    taveDoubles.foreach(println)
    */

    val bins = (-10.0 to 60.0 by 5.0).toArray
    val counts = latSeq(0).filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble/10).histogram(bins, true) 
    val hist = Plot.histogramPlot(bins, counts, RedARGB, false)
    SwingRenderer(hist, 800, 600)

    // val taves = latSeq.map(_.filter(p => p._2.obType == "TMAX" || p._2.obType == "TMIN").map(p =>   )) //.map(p => p._2.obValue.toDouble)).map(_.popStdev)
    






    sc.stop()
}
}