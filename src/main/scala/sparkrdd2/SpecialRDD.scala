package sparkrdd2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle
import org.apache.arrow.flatbuf.Bool
import org.apache.spark.rdd.RDD
// import swiftvis2.spark._

case class StationRow(id:String, lat:Double, lon:Double, elev:Double, state:String, name:String)
case class ReportRow(id:String, year:Int, mmdd:Int, obType:String, obValue:Int, qflag:String)

object SpecialRDD  {

def main(args:Array[String]):Unit = {    
val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
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
   
    val reports2017 = parseReports(sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv"))
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
    */




    sc.stop()
}
}