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
        // val totalRows = bottles.count()
        // val counts = bottles.summary("count").first()
        // val noNulls = (for (i <- 1 to counts.length-1) yield if (counts.get(i).toString.toInt > totalRows / 2) 1 else 0).sum
        // println(noNulls)

        // /* 2. Avg. bottle count per cast */ 
        // val cnts = bottles.groupBy("Cst_Cnt").agg(count($"Btl_Cnt").as("cnts")).agg(avg($"cnts")).show()
        // println("Avg. bottles per count " + cnts)
    
        /* 3. Locations of casts */
        // val newCast = cast.filter('Lat_Dec =!= null && 'Lon_Dec =!= null)
        val preNewCast = cast.filter('Lat_Dec.isNotNull && 'Lon_Dec.isNotNull) 
        val newCast = cast.join(bottles, "Cst_Cnt").filter('Depthm.isNotNull && 'T_degC.isNotNull)
        val longs = newCast.select('Lon_Dec.as[Double]).collect()
        val lats = newCast.select('Lat_Dec.as[Double]).collect()
        val maxMin = newCast.select('Depthm.as[Int].as("d")).agg(max($"d"), min($"d")).first()
        val sz = ColorGradient(maxMin.getInt(0).toDouble -> 1, maxMin.getInt(1).toDouble -> 10)
        val sizes = newCast.select('Depthm.as[Int]).collect().map(d => (d/300.0).toInt + 2)
        // val sizes = newCast.select('Depthm.as[Int]).collect().map(d => sz(d))
        val cg = ColorGradient(0.0 -> BlueARGB, 12.0 -> RedARGB)
        val colors = newCast.select('T_degC.as[Double]).collect().map(t => cg(t))
        val p = Plot.simple(
            ScatterStyle(longs, lats, symbolWidth = sizes, symbolHeight = sizes, colors = colors), 
            "Casts", "Long.", "Lat."
        )
        SwingRenderer(p, 800, 800, true)

    }
}