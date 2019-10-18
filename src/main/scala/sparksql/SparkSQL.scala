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
import org.apache.spark.SparkConf
import swiftvis2.plotting.Plot
import swiftvis2.plotting._
import swiftvis2.plotting.renderer.SwingRenderer
import org.apache.parquet.format.IntType


object SparkSQL extends App {
    val sparkConf = new SparkConf().setAppName("Test")
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    val spark = SparkSession.builder().config(sparkConf).master("local[*]").appName("Temp Data").getOrCreate()
    import spark.implicits._
  
    spark.sparkContext.setLogLevel("WARN")
    val dataSchema = StructType(
        Array(
        StructField("series_id", StringType),
        StructField("year", IntegerType),
        StructField("period", StringType),
        StructField("value", DoubleType),
        StructField("footnote_codes", StringType)
        )
    )

    val dataNM = spark.read.schema(dataSchema).
        option("header", "true").
        option("delimiter", "\t").
    //csv("C:/Users/Dillon/comp/datasets/sparksql/la/la.data.38.NewMexico")
    csv("/data/BigData/bls/la/la.data.38.NewMexico")
    /* 1. How many series does NM have? */
  
    //csv("C:/Users/Dillon/comp/datasets/sparksql/la/la.data.38.NewMexico")

    println("NM has " + dataNM.select('series_id).distinct().count() +" series")

    /* 2. Highest unemployment level reported in any county in New Mexico for the time series */
    
    val areaSchema = StructType(
        Array(
            StructField("area_type_code", StringType),
            StructField("area_code", StringType),
            StructField("area_text", StringType),
            StructField("display_level", StringType),
            StructField("selectable", StringType),
            StructField("sort_sequence", IntegerType)
        )
    )

    val dataArea = spark.read.schema(areaSchema).
        option("header", "true").
        option("delimiter", "\t").
    csv("/data/BigData/bls/la/la.area")
    
    val countiesNM = dataArea.filter('area_type_code === "F" && 'area_code.substr(3,2) === "35")
    val unemployments = dataNM.filter('series_id.substr(19, 2) === "04").select('series_id.substr(4, 15).as("area_code"), 'value) //lol 
    val joined = countiesNM.join(unemployments, "area_code") //.show()
    // joined.describe().show()
    
    /* 3. How many cities/towns with more than 25,000 people does the BLS track in New Mexico? */
    val nmCities = dataArea.filter('area_type_code === "G" && 'area_code.substr(3,2) === "35")
    println("NM cities with >25k population " + nmCities.count())
   
    /* 4. What was the average unemployment rate for New Mexico in 2017? Calculate this in three ways: */
    /* a. Averages of the months for the BLS series for the whole state. */
    val data2017 = dataNM.filter('year === 2017 && 'series_id.substr(19, 2) === "03").groupBy('period).avg().orderBy('period)
    data2017.show()

    /* b. Simple average of all unemployment rates for counties in the state. */
    val rates = dataNM.filter('year === 2017 && 'series_id.substr(19, 2) === "03").select('series_id.substr(4,15).as("area_code"), 'period, 'value.as("rate"))
    val rates_counties = countiesNM.join(rates, "area_code")

    /* 5. Same as 4 */
    /* c. Labor force as weight average */
    val labor2017 = dataNM.filter('year === 2017 && 'series_id.substr(19,2) === "06").select('series_id.substr(4,15).as("area_code"), 'period, 'value.as("labor"))
    val labor_counties = countiesNM.join(labor2017, "area_code")
    val laborWithRates = rates_counties.join(labor_counties, Seq("area_code", "period"))
    // val weightSums = laborWithRates.join(countiesNM, "area_code").groupBy('period).sum("labor").show()
    laborWithRates.show()



    val weightSums = labor2017.groupBy('period).sum("labor")
    val values = rates.groupBy('period)
    // val weightsWithVals = weightSums.


    
}
