package sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sqrt
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import swiftvis2.plotting.Plot
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer


object SparkSQL extends App {
    val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
    import spark.implicits._
  
    spark.sparkContext.setLogLevel("WARN")
    val dataSchema = StructType(
        Array(
        StructField("series_id", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType),
        StructField("footnode_codes", StringType)
        )
    )

    val dataNM = spark.read.schema(dataSchema).
        option("header", "true").
        option("delimiter", "\t").
    csv("C:/Users/Dillon/comp/datasets/sparksql/la/la.data.38.NewMexico")
    
    /* 1. How many series does NM have? */
    println("NM has " + dataNM.select('series_id).distinct().count() +" series")

    /* 2. Highest unemployment level of any county or equivalent in NM? */
    //join with the area data
    //la.series contains stuff, with series ID

    //series has the data for a "series", basically a series of observations and their dates
        //the series ID uniquely identifies a single time series
    //figure out how to group all this data together
}
