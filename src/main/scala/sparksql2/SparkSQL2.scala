package sparksql2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import swiftvis2.DataSet
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import swiftvis2.plotting.styles.ScatterStyle

case class SeriesEntry(series_id:String, year:Int, period:String,
    value:Double, footnote_codes:String)

case class VoteRow(lineNum:Int, votes_dem:Double, votes_gop:Double, 
    total_votes:Double, per_dem:Double, per_gop:Double, diff:String,
    per_point_diff:String, state_abbr:String, county_name:String,combined_fips:Int)

case class VoteData(lineNum:Int, votes_dem:Double, votes_gop:Double, 
    total_votes:Double, per_dem:Double, per_gop:Double, diff:Int,
    per_point_diff:Double, state_abbr:String, county_name:String,combined_fips:Int)

object SparkSQL2 {
    def main(args:Array[String]) = {
      val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("WARN")
      
      val data = spark.read.schema(Encoders.product[SeriesEntry].schema).
        // option("header", "true").
        option("delimiter", "\t").
        csv("/data/BigData/bls/la/la.data.38.NewMexico").as[SeriesEntry]

      val voteRows = spark.read.schema(Encoders.product[VoteRow].schema).
        // option("header", "true").
      csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").as[VoteRow].
      filter($"state_abbr" =!= "AK")
      
      val voteData = voteRows.map(r => VoteData(r.lineNum, r.votes_dem, r.votes_gop, 
          r.total_votes, r.per_dem, r.per_gop, r.diff.filter(_ != ',').toInt, 
      r.per_point_diff.filter(_ != '%').toDouble, r.state_abbr, r.county_name, r.combined_fips))
      
      /* 1. Fraction of counties with gop majority */
      println(voteData.filter(d => d.votes_dem < d.votes_gop).count() / voteData.count().toDouble)
      println(voteData.filter(d => d.per_gop > 0.5).count() / voteData.count().toDouble)
  
      /* 2. Fraction of 10% margin */  
      println(voteData.filter(d => d.per_gop-d.per_dem >= 0.1).count() / voteData.count().toDouble)
      println(voteData.filter(d => d.per_gop-d.per_dem <= 0.1).count() / voteData.count().toDouble)
      
      /* 3. Plot >:( */
      val x1 = voteData.map(_.total_votes).collect()
      val y1 = voteData.map(d => d.per_gop - d.per_dem).collect()
      val p1 = Plot.simple(ScatterStyle(x1, y1, symbolWidth = 8, symbolHeight = 8), "Vote Info of Counties",
      "# Votes", "% Dem - % GOP")

      val x2 = voteData.filter(_.total_votes < 70000).map(_.total_votes).collect()
      val y2 = voteData.filter(_.total_votes < 70000).map(d => d.per_gop - d.per_dem).collect()
      val p2 = Plot.simple(ScatterStyle(x2, y2, symbolWidth = 8, symbolHeight = 8), "Vote Info of Counties",
      "# Votes", "% Dem - % GOP")

      SwingRenderer(p2, 800, 800, true)
      val apowefj = 3
    }
}