package sparksql2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import swiftvis2.DataSet
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.styles.HistogramStyle.DataAndColor
import scalafx.application.ConditionalFeature.Swing

case class SeriesEntry(series_id:String, year:Int, period:String,
    value:Double, footnote_codes:String)

case class SeriesEntryArea(series_id:String, year:Int, period:String,
  value:Double, footnote_codes:String, area_code1:String)  

case class AreaData(area_type_code:String, area_code:String, area_text:String, display_level:String, 
  selectable:String, sort_sequence:String)

case class VoteRow(lineNum:Int, votes_dem:Double, votes_gop:Double, 
    total_votes:Double, per_dem:Double, per_gop:Double, diff:String,
    per_point_diff:String, state_abbr:String, county_name:String,combined_fips:Int)

case class VoteData(lineNum:Int, votes_dem:Double, votes_gop:Double, 
    total_votes:Double, per_dem:Double, per_gop:Double, diff:Int,
    per_point_diff:Double, state_abbr:String, county_name:String,combined_fips:Int)

case class ZipCodeData(zip_code:Int, latitude:Double, longitude:Double, city:String, state:String, county:String)

case class VotesWithGeo(votes_dem:Double, votes_gop:Double, total_votes:Double,
    per_dem:Double, per_gop:Double, state_abbr:String, county_name:String,
latitude:Double, longitude:Double)

case class JoinedStep(county:String, per_dem:Double)
case class RatesCountyVotes(rate:Double, county:String, per_dem:Double)

object SparkSQL2 {
    def main(args:Array[String]) = {
      val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")
      
      // val data = spark.read.schema(Encoders.product[SeriesEntry].schema).
      //   // option("header", "true").
      //   option("delimiter", "\t").
      //   csv("/data/BigData/bls/la/la.data.38.NewMexico").as[SeriesEntry]

      val voteRows = spark.read.schema(Encoders.product[VoteRow].schema).
        // option("header", "true").
      csv("C:\\Users\\Dillon\\comp\\datasets\\sparksql\\2016_US_County_Level_Presidential_Results.csv").as[VoteRow].
        // csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").as[VoteRow].
      filter($"state_abbr" =!= "AK")
      
      val voteData = voteRows.map(r => VoteData(r.lineNum, r.votes_dem, r.votes_gop, 
          r.total_votes, r.per_dem, r.per_gop, r.diff.filter(_ != ',').toInt, 
      r.per_point_diff.filter(_ != '%').toDouble, r.state_abbr, r.county_name, r.combined_fips))
      
      // /* 1. Fraction of counties with gop majority */
      // println(voteData.filter(d => d.votes_dem < d.votes_gop).count() / voteData.count().toDouble)
      // println(voteData.filter(d => d.per_gop > 0.5).count() / voteData.count().toDouble)
  
      // /* 2. Fraction of 10% margin */  
      // println(voteData.filter(d => d.per_gop-d.per_dem >= 0.1).count() / voteData.count().toDouble)
      // println(voteData.filter(d => d.per_gop-d.per_dem <= 0.1).count() / voteData.count().toDouble)
      
      /* 3. Plot >:( */
      /*
      val x1 = voteData.map(_.total_votes).collect()
      val y1 = voteData.map(d => d.per_gop - d.per_dem).collect()
      val p1 = Plot.simple(ScatterStyle(x1, y1, symbolWidth = 8, symbolHeight = 8), "Vote Info of Counties",
      "# Votes", "% Dem - % GOP")

      val x2 = voteData.filter(_.total_votes < 70000).map(_.total_votes).collect()
      val y2 = voteData.filter(_.total_votes < 70000).map(d => d.per_gop - d.per_dem).collect()
      val p2 = Plot.simple(ScatterStyle(x2, y2, symbolWidth = 8, symbolHeight = 8), "Vote Info of Counties",
      "# Votes", "% Dem - % GOP")

      SwingRenderer(p2, 800, 800, true)
      */

      /* 4. Plotting vote results geographically */
      val dataGeo = spark.read.schema(Encoders.product[ZipCodeData].schema).
      option("header", "true").
      option("delimiter", "," ).
      csv("C:\\Users\\Dillon\\comp\\datasets\\sparksql\\zip_codes_states.csv").as[ZipCodeData]
      //csv("/data/BigData/bls/zip_codes_states.csv").as[ZipCodeData]
      .filter("latitude is not null and longitude is not null")
      
      /****THIS MIGHT BE WRONG*****/

    //   val joined1 = voteData.joinWith(dataGeo, 
    //       voteData("state_abbr") === dataGeo("state"))
          
    //   val votesWithGeo = joined1.map { case (vd, geo) =>
    //     VotesWithGeo(vd.votes_dem, vd.votes_gop, vd.total_votes, vd.per_dem, vd.per_gop,
    //       vd.state_abbr, vd.county_name, geo.latitude, geo.longitude)
    //   }//.filter(d => d.latitude != null || d.longitude != null)

    // val cg1 = ColorGradient(0.0 -> RedARGB, 39.9->RedARGB, 
    //   40.0->MagentaARGB, 59.9->MagentaARGB, 60.0->BlueARGB)      
    // val p1 = Plot.simple(ScatterStyle(
    //   votesWithGeo.map(_.longitude).collect(),
    //   votesWithGeo.map(_.latitude).collect(),
    //   symbolWidth = 3, symbolHeight = 3,
    //   colors = votesWithGeo.map(_.per_dem).collect().map(p => cg1(p*100))),
    //   "Magenta is 40 <= percent democratic <= 60", "Longitude", "Latitude")

    // SwingRenderer(p1, 800, 800, true)
    
    /* 5. Historgrams of Recessions */
    //Make a function and paramaterize by month, year, before/after recession, 

    val preDataSeries = spark.read.schema(Encoders.product[SeriesEntry].schema).
      option("header", "true").
      option("delimiter", "\t").
      // csv("/data/BigData/bls/la/la.data.concatenatedStateFiles").as[SeriesEntry]
      csv("C:\\Users\\Dillon\\comp\\datasets\\sparksql\\la\\la.data.concatenatedStateFiles").as[SeriesEntry].
      filter("value is not null")

    val dataSeries = preDataSeries.map(d => SeriesEntryArea(d.series_id.trim(),d.year,d.period,d.value,
      d.footnote_codes, d.series_id.substring(3,18)))

    val dataArea = spark.read.schema(Encoders.product[AreaData].schema).
      option("header", "true").
      option("delimiter", "\t").
      csv("C:\\Users\\Dillon\\comp\\datasets\\sparksql\\la\\la.area").as[AreaData]

    // val bins = (0.1 to 50.0 by 1.0).toArray
    // def getHisto(month:String, year:Int, bOrA:Boolean, area_type:String):DataAndColor = {
    //   val series = dataSeries.filter(
    //     d => d.period==month && d.year==year && d.series_id.takeRight(2) == "03") 
    //   val areas = dataArea.filter(r => r.area_type_code==area_type)
    //   val joined = series.joinWith(areas, series("area_code1") === areas("area_code"))
    //   val counts = joined.map(_._1.value).rdd.histogram(bins, true)
    //   println(month, year, area_type)
    //   DataAndColor(counts, if (bOrA) GreenARGB else RedARGB)
    // }

    // val dateParams = Array(
    //   ("M06", 1990, true), ("M03", 1991, false), 
    //   ("M02", 2001, true), ("M11", 2001, false),
    //   ("M12", 2007, true), ("M06", 2009, false))
    // val areaParams = Array("B", "D", "F")
    // val allParams = for (d <- dateParams; a <- areaParams) yield (d._1, d._2, d._3, a)
    // val histos = (for (a <- areaParams) yield {
    //   (for (d <- dateParams) yield getHisto(d._1, d._2, d._3, a)).toSeq
    // }).toSeq
    // // val histos = getHisto("M12", 2007, true, "B")
    // val grid = Plot.histogramGrid(bins, histos, true, false, "Unemployment Rates", "Date", "Rates")
    // SwingRenderer(grid, 1000, 1000, true)

      /* 6. Unemployment and voting correlation */
      //Correlation coefficient between unemployment and democratic votes for all counties
      //gotta join a county with it's voting percentage
      val counties = dataArea.filter(_.area_type_code == "F")
      val unempRates = dataSeries.filter(_.series_id.takeRight(2) == "03")
      //This is only joining on county names, could possibly include state names

      //join votes and counties, then join the area codes with the series measurements. You'll also have to filter out
      //non-2016 years. Filter out to calculate with the M13 values. Acknowledge that this throws it off by one month, 
      //but shouldn't be a big deal. It's not like a recession happened right after. 
      val countiesAndVotes = counties.joinWith(voteData, counties("area_text") === voteData("county_name")).
        map(d => JoinedStep(d._2.county_name, d.))

      val cvs = countiesAndVotes.joinWith(unempRates, countiesAndVotes)


    val goodLord = ""
  }
}