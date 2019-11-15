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
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import scalafx.scene.effect.BlendMode.Red
import io.netty.handler.codec.redis.RedisArrayAggregator

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
        // val longs = newCast.select('Lon_Dec.as[Double]).collect()
        // val lats = newCast.select('Lat_Dec.as[Double]).collect()
        // val maxMin = newCast.select('Depthm.as[Int].as("d")).agg(max($"d"), min($"d")).first()
        // val sz = ColorGradient(maxMin.getInt(0).toDouble -> 1, maxMin.getInt(1).toDouble -> 10)
        // val sizes = newCast.select('Depthm.as[Int]).collect().map(d => (d/300.0).toInt + 2)
        // // val sizes = newCast.select('Depthm.as[Int]).collect().map(d => sz(d))
        // val cg = ColorGradient(0.0 -> BlueARGB, 12.0 -> RedARGB)
        // val colors = newCast.select('T_degC.as[Double]).collect().map(t => cg(t))
        // val p = Plot.simple(
        //     ScatterStyle(longs, lats, symbolWidth = sizes, symbolHeight = sizes, colors = colors), 
        //     "Casts", "Long.", "Lat."
        // )
        // SwingRenderer(p, 800, 800, true)

        /* 4. Linear regression: temperature -> salinity */
        // val newNewCast = newCast.filter('Salnty.isNotNull)
        // val holUp = newNewCast.select('T_degC.as("temperature"), 'Salnty.as("salinity"))
        // val va = new VectorAssembler()
            // .setInputCols(Array("temperature"))
            // .setOutputCol("featLilWayne")
        // val temps = va.transform(holUp)
        // temps.show(false)

        // val lr = new LinearRegression()
            // .setFeaturesCol("featLilWayne")
            // .setLabelCol("salinity")
        // val lrModel = lr.fit(temps)
        // println(lrModel.coefficients)
        // val fitData = lrModel.transform(temps)
        // val god = fitData.select(expr("salinity - prediction").as("diff")).agg(sum("diff")).first()
        // println("Avg. err: " + god.getDouble(0) / fitData.count())
        // println("Mean abs err: "+lrModel.summary.meanAbsoluteError)
    
        /* 5. LR w/ Depth and O2ml_L */
        val newData = newCast.select('T_degC, 'Salnty.as("salinity"), 'Depthm, 'O2ml_L).filter(row => !row.anyNull)
        val va1 = new VectorAssembler()
            .setInputCols(Array("Depthm", "T_degC", "O2ml_L"))
            .setOutputCol("featLilJon")
        val avengersAssembled = va1.transform(newData)
        val lr1 = new LinearRegression()
            .setFeaturesCol("featLilJon")
            .setLabelCol("salinity")
        val lrModel1 = lr1.fit(avengersAssembled)
        val fitData1 = lrModel1.transform(avengersAssembled)
        fitData1.show()
        fitData1.summary().show()
        println("New avg. err: " + lrModel1.summary.meanAbsoluteError)

        /* 7. Graphing results from #5 */
        val actual = fitData1.select('salinity.as[Double]).collect()
        val expected = fitData1.select('prediction.as[Double]).collect()
        val tempers = fitData1.select('T_degC.as[Double]).collect()
        val deeptoots = fitData1.select('Depthm.as[Int]).map(_.toDouble).collect()
        val cg3 = ColorGradient(-0.26 -> RedARGB, -0.13 -> YellowARGB, 0.0 -> GreenARGB, 0.13 -> YellowARGB, 0.26 -> RedARGB)
        val diffs = actual.zip(expected).map(p => cg3(p._1 - p._2)) //includes colors
        val actualSizes = actual.map(d => ((d*d)/100) - 4) 
        val expectedSizes = expected.map(d => ((d*d)/100) - 4) 
        

        val sp = Plot.stacked(Seq(
            ScatterStyle(tempers, deeptoots, symbolWidth = actualSizes, symbolHeight = actualSizes),
            ScatterStyle(tempers, deeptoots, symbolWidth = expectedSizes, symbolHeight = expectedSizes, colors = diffs)
        ), "Expected vs. actual", "Temperature C", "Depth meters")
        SwingRenderer(sp, 1000, 1000, true)

        /* 6. 3-D LR with Julian day, lat, and lon -> O2ml_L */
        // val lrData1 = newCast.select('Lat_Dec, 'Lon_Dec, 'Julian_Day, 'O2ml_L).filter(row => !row.anyNull)
        // val va2 = new VectorAssembler()
        //     .setInputCols(Array("Lat_Dec", "Lon_Dec", "Julian_Day"))
        //     .setOutputCol("featNickiMinaj")
        // val schoolAssembly = va2.transform(lrData1)
        // val lr2 = new LinearRegression()
        //     .setFeaturesCol("featNickiMinaj")
        //     .setLabelCol("O2ml_L")
        // val lrModel2 = lr2.fit(schoolAssembly)
        // val fitData2 = lrModel2.transform(schoolAssembly)
        // fitData2.summary().show(false)
        // println("Mean error for 6: " + lrModel2.summary.meanAbsoluteError)
    }
}