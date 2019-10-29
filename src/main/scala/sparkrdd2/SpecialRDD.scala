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
      val x = lines 
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
     
      // x.foreach(r => println(r))
      x
  }

  def parseBadReports(lines:RDD[String]): RDD[ReportRow] = {
    val x = lines 
    .map { line =>
      val p = line.split(",")
      try {
      Some(ReportRow(
        p(0),
        p(1).take(4).toInt,
        p(1).takeRight(4).toInt,
        p(2), 
        p(3).toInt,
        p(5)
      ))
      } catch {
        case e:NumberFormatException => {
          println(p)
          None
        }
      }
    }.filter(r => r != None).map(_.get).filter(r => r.qflag == "")
    x
  }
    //}
   
    val reports2017 = parseReports(sc.textFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/ghcn-daily/2017.csv"))
    // val reports2017 = parseReports(sc.textFile("/data/BigData/ghcn-daily/2017.csv"))
    // val reports2017 = parseReports(sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\2017.csv"))
    // val reports1897 = parseReports(sc.textFile("C:\\Users\\Dillon\\comp\\datasets\\sparkRDD\\1897.csv"))

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
    // val stationLines = sc.textFile("/data/BigData/ghcn-daily/ghcnd-stations.txt")
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
    val stationPairs = stationData.map(s => s.id -> s)

    val rPairs = reports2017.map(r => r.id -> r).filter(p => p._1.take(2) == "US")
    val sar = stationPairs.join(rPairs)
    val latGroups = sar.map(p => p._2._1.lat -> p._2._2)
    val lat35 = latGroups.filter(_._1 < 35)
    val lat35and45 = latGroups.filter(p => 35 < p._1 && p._1 < 42)
    val lat45 = latGroups.filter(_._1 > 42)
    val latSeq = Seq(lat35, lat35and45, lat45)
    //make all the pairs first, then later you can filter it all by the desired obType

    val tmaxsDoubs = latSeq.map(_.filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble)).map(_.popStdev)
    tmaxsDoubs.foreach(println)
    val tmins = latSeq.map(_.filter(p => p._2.obType == "TMIN").map(p => (p._2.id, p._2.mmdd) -> p._2))
    val tmaxs = latSeq.map(_.filter(p => p._2.obType == "TMAX").map(p => (p._2.id, p._2.mmdd) -> p._2))
    val tMatches = tmins.zip(tmaxs).map(p => p._1.join(p._2).map(p1 => ((p1._2._2.obValue + p1._2._1.obValue.toDouble)/2)))
    val taveDoubles = tMatches.map(_.popStdev()/10.0)
    taveDoubles.foreach(println)

    /*
    val bins = (-10.0 to 60.0 by 5.0).toArray
    val counts = latSeq(0).filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble/10).histogram(bins, true) 
    val hist = Plot.histogramPlot(bins, counts, RedARGB, false, "Lat < 35", "Degrees Celsuis", "Number of Reports")
    SwingRenderer(hist, 800, 600)
      */
    val bins2 = (-30.0 to 60.0 by 5.0).toArray
    val counts2 = latSeq(1).filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble/10).histogram(bins2, true) 
    val counts69 = latSeq(1).filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble/10) 
    val hist2 = Plot.histogramPlot(bins2, counts2, BlueARGB, false, "35 < Lat < 42", "Degrees Celsuis", "Number of Reports")
    SwingRenderer(hist2, 800, 600)
    val bins3 = (-40.0 to 60.0 by 5.0).toArray
    val counts3 = latSeq(2).filter(p => p._2.obType == "TMAX").map(p => p._2.obValue.toDouble/10).histogram(bins3, true) 
    val hist3 = Plot.histogramPlot(bins3, counts3, GreenARGB, false, "42 < Lat", "Degrees Celsuis", "Number of Reports")
    SwingRenderer(hist3, 800, 600)
      /*
    val cg = ColorGradient(100.0 -> RedARGB, 0.0 -> BlueARGB, 50.0 -> GreenARGB)

    println(cg(40))
    println(cg(51))
    println(cg(101))
    println("70: " + cg(70))

    val tmax2017 = reports2017.filter(r => r.obType == "TMAX")
    
    val allSar = tmax2017.map(r => r.id -> r).join(stationPairs).map(p => p._1 -> (p._2._2.lon, p._2._2.lat))
    val avgs = tmax2017.map(r => r.id -> (r.obValue, 1)).reduceByKey{
      case ((t1, c1), (t2, c2)) =>
        (t1+t2, c1+c2)
    }.map(p => p._1 -> (p._2._1 / p._2._2.toDouble))
    val allData = allSar.join(avgs).map(p => p._2)
    val cols = allData.collect.map(t => cg(((t._2/10.0) * (9.0/5)) + 32)).toArray
    
    val plot = Plot.simple(
      ScatterStyle(allData.map(_._1._1).collect().toArray.map(_.toDouble), allData.map(_._1._2).collect().toArray.map(_.toDouble), 
      symbolWidth = 3, symbolHeight = 3, colors = cols), "2017 Temps", "Longitude", "Latitude")
    
    SwingRenderer(plot, 1000, 1000)
    */
    
    /* 7. Change of average land temperature */
    //Avg. 1897 Temps
    
    def calcAvgTemp(reports:RDD[ReportRow]):RDD[(String, (Double, Double))] = {
        val reportPairs = reports.map(r => r.id -> r)
        val tmins = reportPairs.filter(p => p._2.obType == "TMIN")
        val tmaxs = reportPairs.filter(p => p._2.obType == "TMAX")
        val tminAvgs = tmins.map(r => r._1 -> (r._2.obValue, 1)).reduceByKey{
          case ((t1, c1), (t2, c2)) =>
            (t1+t2, c1+c2)
        }.map(p => p._1 -> (p._2._1 / p._2._2.toDouble))
        val tmaxAvgs = tmaxs.map(r => r._1 -> (r._2.obValue, 1)).reduceByKey{
          case ((t1, c1), (t2, c2)) =>
            (t1+t2, c1+c2)
        }.map(p => p._1 -> (p._2._1 / p._2._2.toDouble))
        tminAvgs.join(tmaxAvgs)

    }
    //tmin is first 
    /*
    val avgs1897 = calcAvgTemp(reports1897)
    val avgs2017 = calcAvgTemp(reports2017)
    val tminAvg97 = avgs1897.map(p => p._2._1).sum() / avgs1897.count()
    val tmaxAvg97 = avgs1897.map(p => p._2._2).sum() / avgs1897.count()
    val tminAvg17 = avgs2017.map(p => p._2._1).sum() / avgs2017.count()
    val tmaxAvg17 = avgs2017.map(p => p._2._2).sum() / avgs2017.count()
    println("1897 avg. min temperature: " + tminAvg97 + " max: " + tmaxAvg97 )
    println("2017 avg. min temperature: " + tminAvg17 + " max: " + tmaxAvg17 )

    val filter1 = reports1897.filter(r => r.obType == "TMIN")
    val tminAvg97_2 = filter1.map(r => r.obValue).sum() / filter1.count()
    val filter2 = reports1897.filter(r => r.obType == "TMAX")
    val tmaxAvg97_2 = filter2.map(r => r.obValue).sum() / filter2.count()

    val filter3 = reports2017.filter(r => r.obType == "TMIN")
    val tminAvg17_2 = filter3.map(r => r.obValue).sum() / filter3.count()
    val filter4 = reports2017.filter(r => r.obType == "TMAX")
    val tmaxAvg17_2 = filter4.map(r => r.obValue).sum() / filter4.count()

    println("Use this one below")
    println("1897 avg. min temperature: " + tminAvg97_2 + " max: " + tmaxAvg97_2 )
    println("2017 avg. min temperature: " + tminAvg17_2 + " max: " + tmaxAvg17_2 )
    println("divide by 10")

    println("Only 1897 and 2017 reporters")
    val both = reports1897.map(_.id).intersection(reports2017.map(_.id)).collect().toSet
    val filter5 = reports1897.filter(r => r.obType == "TMIN" && both.contains(r.id))
    val tminAvg97_3 = filter5.map(r => r.obValue).sum() / filter5.count()
    val filter6 = reports1897.filter(r => r.obType == "TMAX" && both.contains(r.id))
    val tmaxAvg97_3 = filter6.map(r => r.obValue).sum() / filter6.count()

    val filter7 = reports2017.filter(r => r.obType == "TMIN" && both.contains(r.id))
    val tminAvg17_3 = filter7.map(r => r.obValue).sum() / filter7.count()
    val filter8 = reports2017.filter(r => r.obType == "TMAX"&& both.contains(r.id))
    val tmaxAvg17_3 = filter8.map(r => r.obValue).sum() / filter8.count()
    println("1897 avg. min temperature: " + tminAvg97_3 + " max: " + tmaxAvg97_3 )
    println("2017 avg. min temperature: " + tminAvg17_3 + " max: " + tmaxAvg17_3 )
    println("divide by 10")




    val joined = avgs2017.join(avgs1897)
    val tmindiff = joined.map(p => p._2._1._1-p._2._2._1)
    val tmaxdiff = joined.map(p => p._2._1._2-p._2._2._2)
    println("Avg tmin diff" + tmindiff.sum() / tmindiff.count())
    println("Avg tmax diff" + tmaxdiff.sum() / tmaxdiff.count())

    def getAvgForYear(rdd:RDD[ReportRow]):(Double, Double) = { //first is tmin, second is tmax
      val filter1 = rdd.filter(r => r.obType == "TMIN")
      val tminAvg = filter1.map(r => r.obValue).sum() / filter1.count()
      val filter2 = rdd.filter(r => r.obType == "TMAX")
      val tmaxAvg = filter2.map(r => r.obValue).sum() / filter2.count()
      (tminAvg/10.0, tmaxAvg/10.0)
    }
    
    
    val datas = for (i <- 1897 to 2017 by 10) yield (i -> parseBadReports(sc.textFile("/data/BigData/ghcn-daily/"+i+".csv")))//.map(r => r.year -> r)
    println("All done parsing")
    // 7c plot, tmins
    val tminAvgs = datas.map(r => r._1 -> getAvgForYear(r._2)._1)
    val minScatter = ScatterStyle(tminAvgs.map(_._1).toArray.map(_.toDouble), tminAvgs.map(_._2).toArray.map(_.toDouble), 
      symbolWidth = 8, symbolHeight = 8, colors = BlueARGB)
    val tmaxAvgs = datas.map(r => r._1 -> getAvgForYear(r._2)._2)
    val maxScatter = ScatterStyle(tmaxAvgs.map(_._1).toArray.map(_.toDouble), tmaxAvgs.map(_._2).toArray.map(_.toDouble), 
      symbolWidth = 8, symbolHeight = 8, colors = GreenARGB)

    val tPlot = Plot.stacked(Seq(minScatter, maxScatter), "TMIN is blue, TMAX is green", "Year", "Degrees Celsius")
    SwingRenderer(tPlot, 800, 800)

    // val allReporters = datas.foldLeft(Set[String]())((s, d) => s ++ d._2.map(_.id).collect().toSet)
    
    // val allReporters = datas.redu
    // val datas2 = datas.map(p => p._1 -> p._2.filter(r => allReporters.contains(r.id)))
    val ids = datas.map(r => r._2.map(_.id)).reduce((x, y) => y.intersection(x)).collect().toSet
    // val ids = datas.map(r => r._2.map(_.id)).reduceLeft((x,y) => x.intersection(y)).collect().toSet
    // println(ids.size)

    // var ids1 = datas(0)._2.map(_.id)
    // for (i <- datas) ids1 = ids1.intersection(i._2.map(_.id))
    // val ids = ids1.collect().toSet

    */


    /*
    println("IDs finished")
    val datas2 = datas.map(p => p._1 -> p._2.filter(r => ids.contains(r.id)))
    //val datas2 = datas
    println("Data filtered")
    val tminAvgs2 = datas2.map(r => r._1 -> getAvgForYear(r._2)._1)
    println("tminAvgs2 finished")
    val minScatter2 = ScatterStyle(tminAvgs2.map(_._1).toArray.map(_.toDouble), tminAvgs2.map(_._2).toArray.map(_.toDouble), 
      symbolWidth = 8, symbolHeight = 8, colors = BlueARGB)
    println("minScatter finished")
    val tmaxAvgs2 = datas2.map(r => r._1 -> getAvgForYear(r._2)._2)
      println("tmaxAvgs finished")
    val maxScatter2 = ScatterStyle(tmaxAvgs2.map(_._1).toArray.map(_.toDouble), tmaxAvgs2.map(_._2).toArray.map(_.toDouble), 
      symbolWidth = 8, symbolHeight = 8, colors = GreenARGB)
    println("maxScatter finished")
    val tPlot2 = Plot.stacked(Seq(minScatter2, maxScatter2), "TMIN is blue, TMAX is green", "Year", "Degrees Celsius")
    println("plot mkaer finished")

    SwingRenderer(tPlot2, 800, 800, true)
    */

    






    sc.stop()
}
}