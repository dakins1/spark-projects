package sparkrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
// import swiftvis2.spark._

object SparkRDDTemplate  {

def main(args:Array[String]):Unit = {    
val conf = new SparkConf().setAppName("Temp Data").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  sc.setLogLevel("WARN")
  
  // Your code here. 
  
  sc.stop()
}
}