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
	def main(args: Array[String]): Unit = {
      val source = scala.io.Source.fromFile("/users/mlewis/workspaceF18/CSCI3395-F18/data/BasicScala/IHME_GLOBAL_EDUCATIONAL_ATTAINMENT_1970_2015_Y2015M04D27.CSV")
      val lines = source.getLines()
      val data = lines.drop(1).map(parseLine).toArray
      for (i <- 1 to 10) println(data(i))
    }
}