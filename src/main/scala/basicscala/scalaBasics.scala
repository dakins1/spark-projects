package basicscala

import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting.renderer.SwingRenderer

/**
 * This is here to remind you how to write Scala and to make it so that
 * the directories for src actually go into the git repository.
 */
case class Row(loc_id:Int, loc_code:String, loc_name:String, year:Int, age_id:Int, age_name:String, sex_id:Int, sex_name:String, metric:String, unit:String,
               mean:Double, upper:Double, lower:Double)

case class GdpRow(country:String, code:String, indic_name:String, indic_code:String, gdps:scala.collection.immutable.Map[Int, Option[Double]])

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
   if (prep.length > 62) println("Uh oh" + prep(0))
   val name = prep(0)
   val code = prep(1)
   val indic = prep(2)
   val indicCode = prep(3)
  val gdp = prep.drop(4)
  var i = 0;
  
  val (map, _) = gdp.foldLeft((scala.collection.immutable.Map.empty[Int, Option[Double]], 1960)){
    case ((m, yr), gVal) => 
    if (!gVal.isEmpty) (m+(yr -> Some(gVal.toDouble)), yr+1) else (m+(yr->None), yr+1)
  }
  println(name)
  map.toSeq.sortBy(_._1).foreach(println)
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
      epcs.sortBy(_.upper).takeRight(5).foreach(println)

      /*Largest Increase*/
      //go find all the lowest and highest values, then calculate the difference -- use map that keeps track of largest value (like rainy temp thing)

      /* 4. Largest GDP in 1970 */
      val source1 = scala.io.Source.fromFile("/mnt/c/Users/Dillon/comp/datasets/scalaBasics/API_NY.GDP.PCAP.KD_DS2_en_csv_v2_10081022.csv")
      val lines1 = source1.getLines()
      val data1 = lines1.drop(5).map(parseGdp).toArray
      //data1.map(_.foreach(println))


      source.close()
      source1.close()
    }


}