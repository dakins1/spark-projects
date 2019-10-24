package sparksql

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


object SparkSQL extends App {
    val sparkConf = new SparkConf().setAppName("Test")
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    val spark = SparkSession.builder().config(sparkConf).master("spark://pandora00:7077").appName("Temp Data").getOrCreate()
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
    
    /********************************


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
    /*
    val labor2017 = dataNM.filter('year === 2017 && 'series_id.substr(19,2) === "06").select('series_id.substr(4,15).as("area_code"), 'period, 'value.as("labor"))
    val labor_counties = countiesNM.join(labor2017, "area_code")
    val laborWithRates = rates_counties.join(labor_counties, Seq("area_code", "period"))
    val laborByRatesSum = laborWithRates.select(('rate * 'labor).as("product")).groupBy().sum("product") 
    val laborSum = laborWithRates.groupBy().sum("labor")
    println(laborByRatesSum.first().getDouble(0) / laborSum.first().getDouble(0))
    */

    val labor2017 = dataNM.filter('year === 2017 && 'series_id.substr(19,2) === "06").select('series_id.substr(1,19).as("series_id"), 'period, 'value.as("labor"))
    val labor_counties = countiesNM.join(labor2017).filter('series_id.substr(4,15) === 'area_code) //(labor2017, "area_code")

    val rates1 = dataNM.filter('year === 2017 && 'series_id.substr(19, 2) === "03").select('series_id.substr(1,19).as("series_id"), 'period, 'value.as("rate"))
    val rates_counties1 = countiesNM.join(rates1).filter('series_id.substr(4,15) === 'area_code)

    val laborWithRates = rates_counties1.join(labor_counties, Seq("series_id", "period"))
    val laborByRatesSum = laborWithRates.select(('rate * 'labor).as("product")).groupBy().sum("product") 
    val laborSum = laborWithRates.groupBy().sum("labor")
    println("My avg " + laborByRatesSum.first().getDouble(0) / laborSum.first().getDouble(0))
    val blsAvg = dataNM.filter('year === 2017 && 'series_id.substr(19,2) === "03" && 'period === "M13" && 'series_id.substr(4,15) === "ST3500000000000")
    println("BLS avg")
    blsAvg.show()

    /* 6. What is the highest unemployment rate for a series with a labor force of at least 10,000 people in the state of Texas? When and where? */

    val dataTX = spark.read.schema(dataSchema).
    option("header", "true").
    option("delimiter", "\t").
    //csv("C:/Users/Dillon/comp/datasets/sparksql/la/la.data.51.Texas")
    csv("/data/BigData/bls/la/la.data.51.Texas")

    val series_labor10k = dataTX.filter('series_id.substr(19,2) === "06" && 'value >= 10000).select('series_id.substr(1,19).as("series_id"), 'period, 'year)
    val series_unemployment = dataTX.filter('series_id.substr(19,2) === "03").select('series_id.substr(1,19).as("series_id"), 'period, 'year, 'value)
    val joined2 = series_labor10k.join(series_unemployment, Seq("period", "year", "series_id"))
    println(series_labor10k.count())
    println(series_unemployment.count())
    println(joined2.count())
    val rw = joined2.orderBy(desc("value")).limit(1).first()
    println(rw)
    println(rw.getString(2).drop(3))
    dataArea.filter('area_code === rw.getString(2).drop(3).dropRight(1)).show()

    *///////////////////////////////////////


    /* 7. Same as 6, but with all states. */
    val dataTemp = spark.read.schema(dataSchema).
    option("header", "true").
    option("delimiter", "\t").
    //csv("C:/Users/Dillon/comp/datasets/sparksql/la/la.data.51.Texas")
    csv("/data/BigData/bls/la/la.data.concatenatedStateFiles")
    

    /*
    val series_all = dataTemp.filter('series_id.substr(19,2) === "06" && 'value >= 10000).select('series_id.substr(1,19).as("series_id"), 'period, 'year)
    val unemp_rates = dataTemp.filter('series_id.substr(19,2) === "03" && 'value >= 54.1).select('series_id.substr(1,19).as("series_id"), 'period, 'year, 'value)
    val joined3 = series_all.join(unemp_rates, Seq("period", "year", "series_id"))
    println(series_all.count())
    println(unemp_rates.count())
    println(joined3.count())
    val rw2 = joined3.orderBy(desc("value")).limit(1).first()
    println(rw2)
    println(rw2.getString(2).drop(3))
    dataArea.filter('area_code === rw2.getString(2).drop(3).dropRight(1)).show()
    */
    // joined2.agg(max($"value")).show()
    //TX has state code of 48
    //make sure to join on series id (minus the ob code), month and year

    //for number 7, filter everything less than number 6

    /* 8. What state has the most distinct data series? */
    /*
    val groups = dataTemp.select('series_id.substr(6,2).as("state"), 'series_id).groupBy("state").agg(countDistinct("series_id").as("count"))
    val maxCode = groups.orderBy(desc("count")).limit(1).first()
    println("State with most distinct series: "+maxCode)
    */


    /* 9. Geographic plots, 2000 to 2015 by 5 */
    //have to match up with the zip codes file, probs match the zip code's city name as a substring of the bls area name
    //Ex. zip code file says "Harvard", blw file says "harvard town"
    //maybe match on state before hand, too, just to reduce the likelihood of overlap

    //2000
    val geoSchema = StructType(
        Array(
            StructField("zip_code", IntegerType),
            StructField("latitude", DoubleType),
            StructField("longitude", DoubleType),
            StructField("city", StringType),
            StructField("state", StringType),
            StructField("country", StringType)
        )
    )

    val dataGeo = spark.read.schema(geoSchema).
    option("header", "true").
    option("delimiter", ",").
    //csv("C:/Users/Dillon/comp/datasets/sparksql/la/la.data.51.Texas")
    csv("/data/BigData/bls/zip_codes_states.csv").filter('state =!= "PR" || 'state =!= "HI" || 'state =!= "AK")

    // val stateCodes = Array("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL", "IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND", "OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY") 
    // val stateNames = scala.io.Source.fromFile("/users/dakins1/bigdata/stateName.txt").getLines().toArray
    // val stateMap = stateCodes.zip(stateNames).toMap
    val stateSchema = StructType(Array(StructField("code", StringType), StructField("name", StringType)))
    val stateMap = spark.read.schema(stateSchema).option("header", "false").option("delimiter", ",").csv("/users/dakins1/bigdata/stateMappings.txt")

    val unempRates = dataTemp.filter('series_id.substr(19,2) === "03")
    val joined4 = dataGeo.join(dataArea.filter(!'area_text.contains("Alaska") || !'area_text.contains("Hawaii") || !'area_text.contains("Puerto Rico") || 
        !'area_text.contains("AK") || !'area_text.contains("PR") || !'area_text.contains("HI")), 
        'area_text.contains('city) && 'area_text.contains('state))
    // joined4.show()
    val joined5 = joined4.join(unempRates, 'area_code === 'series_id.substr(4, 15))

    unempRates.describe().show()

    val cg = ColorGradient(1946.0 -> RedARGB, 1975.0 -> BlueARGB, 2014.0 -> GreenARGB)
    // val sizes = data.map(_.precip * 2 + 2)
    // val tempByDayPlot = Plot.simple(
    //   ScatterStyle(data.map(_.doy), data.map(_.tave), symbolWidth = sizes, symbolHeight = sizes, colors = cg(data.map(_.year))), 
    //   "SA Temps", "Day of Year", "Temp")
    // SwingRenderer(tempByDayPlot, 800, 800, true)

    //how do we account for multiple entries of the same city...?

    val fuckMe = dataArea.filter(!'area_text.contains("Alaska") || !'area_text.contains("Hawaii") || !'area_text.contains("Puerto Rico") || 
        !'area_text.contains("AK") || !'area_text.contains("PR") || !'area_text.contains("HI"))
    println(fuckMe.count())
    fuckMe.show(fuckMe.count().toInt)

    // 49.029970, -126.697805
    // 23.767838, -72.197626
    def makeThingy(data:org.apache.spark.sql.DataFrame):(PlotDoubleSeries, PlotDoubleSeries, PlotIntSeries, PlotDoubleSeries) = {
        /*
        val cg = ColorGradient(6+(3.5*3) -> RedARGB, 6.0 -> BlueARGB, 1.0 -> GreenARGB)
        val arr = data.select('latitude, 'longitude, 'value).filter("longitude is not null or latitude is not null").collect()
        println(arr(3))
        val sizes =  (for (i <- 1 to arr.size) yield 8).toArray
        // for (i <- 310 to 400) println(i + " " + data.
        (arr.map(r => r.getDouble(0)), arr.map(_.getDouble(1)), sizes ,arr.map(r => cg(r.getDouble(2))))
        */
        val cg = ColorGradient(6+(3.5/* *3 */) -> RedARGB, 6.0 -> BlueARGB, 1.0 -> GreenARGB)
        val arr = data.select('latitude, 'longitude, 'value)
            .filter("longitude is not null or latitude is not null or (longitude > -126.0 and latitude < 49.0) or (longitude < -72.1 and latitude > 23.7) ").collect()
        println(arr(3))
        val sizes =  (for (i <- 1 to arr.size) yield 8).toArray
        // for (i <- 310 to 400) println(i + " " + data.
        // ScatterStyle(data.map(_.doy), data.map(_.tave), symbolWidth = sizes, symbolHeight = sizes, colors = cg(data.map(_.year))), 
    //   "SA Temps", "Day of Year", "Temp")
        (arr.map(r => r.getDouble(1)), arr.map(_.getDouble(0)), sizes, arr.map(r => cg(r.getDouble(2))))
    }
    
    def makeThingy1(data:org.apache.spark.sql.DataFrame):ScatterStyle = {
        val cg = ColorGradient(6+(3.5/* *3 */) -> RedARGB, 6.0 -> BlueARGB, 1.0 -> GreenARGB)
        val arr = data.select('latitude, 'longitude, 'value)
            .filter(('longitude > -126.0 && 'latitude < 49.0) || ('longitude < -72.1 && 'latitude > 23.7)).collect()
        println(arr(3))
        val sizes =  (for (i <- 1 to arr.size) yield 8).toArray
        // for (i <- 310 to 400) println(i + " " + data.
        // ScatterStyle(data.map(_.doy), data.map(_.tave), symbolWidth = sizes, symbolHeight = sizes, colors = cg(data.map(_.year))), 
    //   "SA Temps", "Day of Year", "Temp")
        ScatterStyle(arr.map(r => r.getDouble(1)), arr.map(_.getDouble(0)), symbolWidth = 8 , symbolHeight = 8, colors = arr.map(r => cg(r.getDouble(2))))
    }

    val y2ks = (for (i <- 2000 to 2015 by 5) yield joined5.filter('year === i)).toSeq
    

    // val plots = Plot.scatterPlots(y2ks.map(y => makeThingy(y)), "Data" , "Longitude", "Latitude") 
    // val plots = Plot.scatterPlotGrid(Seq(makeThingy(y2k5)), "Data" , "Longitude", "Latitude") 
    //try flipping rows/columns next
        val p = Plot.simple(makeThingy1(y2ks(0)), "Unemployment Rates 2000", "Longitude", "Latitude")
        SwingRenderer(p, 800, 800, true)
        val p1 = Plot.simple(makeThingy1(y2ks(1)), "Unemployment Rates 2005", "Longitude", "Latitude")
        SwingRenderer(p1, 800, 800, true)
        val p2 = Plot.simple(makeThingy1(y2ks(2)), "Unemployment Rates 2010", "Longitude", "Latitude")
        SwingRenderer(p2, 800, 800, true)
        val p3 = Plot.simple(makeThingy1(y2ks(3)), "Unemployment Rates 2015", "Longitude", "Latitude")
        SwingRenderer(p3, 800, 800, true)
    
    





    //filter alaska, hawaii, and puerto rico
    //probs have to map the state code to the state abbrev.

}
