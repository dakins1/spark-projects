package basicscala
import swiftvis2.plotting._
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer


/**
 * This is here to remind you how to write Scala and to make it so that
 * the directories for src actually go into the git repository.
 */
case class Row(loc_id:Int, loc_code:String, loc_name:String, year:Int, age_id:Int, age_name:String, sex_id:Int, sex_name:String, metric:String, unit:String,
               mean:Double, upper:Double, lower:Double)

case class GdpRow(name:String, code:String, indic_name:String, indic_code:String, gdps:scala.collection.immutable.Map[Int, Option[Double]])

object ScalaBasics {

  def parseLine(line: String): Row = {
    if (line.contains("\"")) {
      val prep = line.split("\"")
      val p = prep(0).split(",") ++ Array(prep(1)) ++ prep(2).split(",").drop(1)    
      Row(p(0).toInt, p(1), p(2), p(3).toInt, p(4).toInt, p(5).toString, 
            p(6).toInt, p(7), p(8), p(9), p(10).toDouble, p(11).toDouble, p(12).toDouble) 
    } else {
    val p = line.split(",")
    Row(p(0).toInt, p(1), p(2), p(3).toInt, p(4).toInt, p(5).toString, 
            p(6).toInt, p(7), p(8), p(9), p(10).toDouble, p(11).toDouble, p(12).toDouble)
    }
  }

  def parseGdp(line:String): GdpRow = {
    val prePrep = line.split(",")
    val prep = {
     if (prePrep.length > 62) line.replaceFirst(",", "").split(",")
     else line.split(",")
   }.map(_.filter(c => c != '"')) 
   val name = prep(0)
   val code = prep(1)
   val indic = prep(2)
   val indicCode = prep(3)
  val gdp = prep.drop(4)
  val (map, _) = gdp.foldLeft((scala.collection.immutable.Map.empty[Int, Option[Double]], 1960)){
    case ((m, yr), gVal) => 
    if (!gVal.isEmpty) (m+(yr -> Some(gVal.toDouble)), yr+1) else (m+(yr->None), yr+1)
  }
   GdpRow(name, code, indic, indicCode, map)
  }   

	def main(args: Array[String]): Unit = {
      //val source = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
      val source = scala.io.Source.fromFile("/mnt/c/Users/Dillon/comp/datasets/scalaBasics/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
      val lines = source.getLines()
      val data = lines.drop(1).map(parseLine).toArray
      val metrics = scala.collection.mutable.Set.empty[String]
      data.map(d => metrics += d.metric)
      
      /*Highest Edu Per Capita Values*/
      val epcs = data.filter(_.metric == "Education Per Capita")
      //get each a country a map of its values
      
      /* 2. 5 Highest Countries */
      //epcs.sortBy(_.upper).takeRight(5).foreach(println)

      /*Largest Increase*/
      //go find all the lowest and highest values, then calculate the difference -- use map that keeps track of largest value (like rainy temp thing)
      //case class EduInfo(name:String, vals:Array[Double]) 

      def mapify(rows: Array[Row]): Map[String, Map[Int, Double]] = {
        val freshMap = rows.map(r => (r.loc_name+" "+r.age_name +" "+r.sex_name) -> (Map.empty[Int, Double])).toMap
        rows.foldLeft(freshMap)
        {(countryMap, r) => {
          val id = (r.loc_name+" "+r.age_name +" "+r.sex_name)
          val yearMap = countryMap(id)
          val newMap = yearMap + (r.year -> r.upper)
          countryMap + (id -> newMap)
        }
      }
      }
      case class CountryEdu(id:String, vals:Map[Int, Double])
      val info = mapify(epcs).foldLeft(List[CountryEdu]())((lst, m) => CountryEdu(m._1, m._2) :: lst ).toArray
      case class MinMax(name:String, min:Double, minYr:Int, max:Double, maxYr:Int, diff:Double)
      val eduMimas = info.foldLeft(List[MinMax]()){
        (lst, cedu) =>
        val (minYr, minOp) = cedu.vals.minBy{case (k,v) => v}
        val (maxYr, maxOp) = cedu.vals.maxBy{case (k,v) => v}
          val min = minOp
          val max = maxOp
          MinMax(cedu.id, min, minYr, max, maxYr, max - min) :: lst
      }
      val eduMaxBoi = eduMimas.maxBy(_.diff)
      println("Max edu diff: " + eduMaxBoi)

      /* 4 & 5 Largest GDP in 1970 */
      val source1 = scala.io.Source.fromFile("/mnt/c/Users/Dillon/comp/datasets/scalaBasics/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022.csv")
      val lines1 = source1.getLines()
      val gRows = lines1.drop(5).map(parseGdp).toArray
      val g1 = gRows.maxBy(_.gdps(1970).getOrElse(-1.0))
      val h1 = g1.gdps(1970).get
      println("Highest in 1970: " + g1.name + " " + g1.gdps(1970).get)
      val g2 = gRows.minBy(_.gdps(1970).getOrElse(h1))
      println("Lowest in 1970: " + g2.name + " " + g2.gdps(1970).get)
      val g3 = gRows.maxBy(_.gdps(2015).getOrElse(-1.0))
      val h2 = g1.gdps(1970).get
      println("Highest in 2015: " + g3.name + " " + g1.gdps(2015).get)
      val g4 = gRows.minBy(_.gdps(2015).getOrElse(h2))
      println("Lowest in 2015: " + g4.name + " " + g2.gdps(2015).get)

      /* 6. Largest GDP Increase from 1970 to 2015 */ 
     // case class MinMax(name:String, min:Double, minYr:Int, max:Double, maxYr:Int, diff:Double)
      val mimas = gRows.foldLeft(List[MinMax]()){
        (lst, row) =>
        val (minYr, minOp) = row.gdps.minBy{case (k,v) => v.getOrElse(100000000.0)}
        val (maxYr, maxOp) = row.gdps.maxBy{case (k,v) => v.getOrElse(-1.0)}
        if (minOp != None) {
          val min = minOp.get
          val max = maxOp.get
          MinMax(row.name, min, minYr, max, maxYr, max - min) :: lst
        } else lst       
      }
      val maxBoi = mimas.maxBy(_.diff)
      println("Max diff: " + maxBoi)

      /* 7. Plot of 3 countries, Females 25-34 by year */
      val cg = ColorGradient(1946.0 -> RedARGB, 1975.0 -> BlueARGB, 2014.0 -> GreenARGB)
      // val sizes = info.map(_.vals.map(_._2 * 2 + 2))
      val tempByDayPlot = Plot.simple(
        ScatterStyle(info(4).vals.map(_._1).toArray[Int], info(4).vals.map(_._2).toArray[Double]), 
      "SA Temps", "Day of Year", "Temp")
      SwingRenderer(tempByDayPlot, 800, 800, true)

      source.close()
      source1.close()
    }
  }