package sparkml

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sqrt
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.SparkConf
import swiftvis2.plotting.Plot
import swiftvis2.plotting.styles.ScatterStyle
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import org.apache.parquet.format.IntType

object MLRegression {
    def main(args:Array[String]) {
        val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")

        val bottles = spark.read.
        option("header", "true").
        option("inferSchema", true).
        csv("C:/Users/Dillon/comp/datasets/oceans/bottle.csv")
    
        val cast = spark.read.
        option("header", "true").
        option("inferSchema", true).
        csv("C:/Users/Dillon/comp/datasets/oceans/cast.csv")

        /* 1. How many columns in bottle.csv have data for at least half the rows? */ 
        // bottles.summary("count").show()
        val totalRows = bottles.count()
        val counts = bottles.summary("count").first()
        val noNulls = (for (i <- 1 to counts.length-1) yield if (counts.get(i).toString.toInt > totalRows / 2) 1 else 0).sum
        println(noNulls)
    }
}