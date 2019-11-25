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
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StandardScaler

object MLClustering {
    def main(args:Array[String]) {

        
        val spark = SparkSession.builder().master("local[*]").appName("Temp Data").getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")

        val dataQcew = spark.read.
        option("header", "true").
        option("inferSchema", true).
        csv("C:/Users/Dillon/comp/datasets/sparksql/qcew/2016.q1-q4.singlefile.csv")

        val dataVotes = spark.read.
        option("header", "true").
        option("inferSchema", true).
        csv("C:/Users/Dillon/comp/datasets/sparksql/2016_US_County_Level_Presidential_Results.csv").
        filter('state_abbr =!= "AK")

        val dataCounty = spark.read.
        option("header", "true").
        option("inferSchema", true).
        csv("C:/Users/Dillon/comp/datasets/sparksql/qcew/area_titles.csv")


        /* 1. Which aggregation level codes are for county-level data? 
        How many entries are in the main data file for each of the county level codes? */
        
        // val cts = dataQcew.filter('agglvl_code >= 70 && 'agglvl_code <= 78)
        // cts.groupBy('agglvl_code).agg(count('agglvl_code)).show()
        // println("County related data: " + cts.count())

        /* 2. How many for bexar county? 48029 */
        // val brcty = dataQcew.filter('area_fips === 48029).count()
        // println("Bexar county entries: " + brcty)

        /* 3. Three most comment industry codes, # of records for each */
        // val indCds = dataQcew.groupBy($"industry_code").agg(count($"industry_code").as("cnt")).orderBy(desc("cnt")).limit(3)
        // indCds.show()

        /* 4. Top three industries by total wage? What are those wages? Consider only NAICS 6-digit county code */
        // val wgs = dataQcew.filter('agglvl_code === 78).groupBy($"industry_code").agg(sum($"total_qtrly_wages").as("wgs")).
        //     orderBy(desc("wgs")).limit(3)
        // wgs.show()        

        /* 5. Find clusters that align with voting tendencies */
        //align with voting tendencies
        //probs use color to notate voting tendencies -- actually no, use color to show the cluster it was placed in
        //but then make one of the axes voting tendency or sumtin...fack

        //ah, so you're not picking 2-3 dimensions, you're trying to find an ML clustering instance that results in 
        //2 clusters, and then 3 or more clusters
        //For the three or more, neutral, moderate, and extreme might be cool clusters
        
        //then regress. Try making one of the features the industry code,
        //mite b coo'

        //join with area titles
        println("**********BEFORE JOIN**************")
        val joined = dataCounty.
            join(dataVotes, 'area_title.contains('county_name)).
            join(dataQcew, "area_fips").
            filter(row => !row.anyNull).
            select('total_qtrly_wages, 'month1_emplvl, 'qtrly_estabs, 'per_dem) //selecting only necessary values hopefully helps
        // val featuredData = joined.select('total_qtrly_wages, 'month1_emplvl, 'qtrly_estabs)
        println("*****AFTER JOIN*************\n\n\n\n")
        val va = new VectorAssembler()
            .setInputCols(Array("total_qtrly_wages"))
            .setOutputCol("featRihanna")
        val dataWithFeatures = va.transform(joined) //giving it all the data so we can compare cluster with vote data
        println("**********AFTER ASSEMBLER*************")
        val scaler = new StandardScaler()
            .setInputCol("featRihanna")
            .setOutputCol("featFuture")
        val scalerModel = scaler.fit(dataWithFeatures)
        val scaledData = scalerModel.transform(dataWithFeatures).cache()
        println("**************AFTER SCALER*****************")
        println("*************START KMEANS*****************")
        val kmeans = new KMeans().setK(2).setFeaturesCol("featFuture")
        val kmeansModel = kmeans.fit(scaledData)
        val dataWithClusters = kmeansModel.transform(scaledData)
        
        // val pdata = dataWithClusters.select('total_qtrly_wages.as[Double], 'month1_emplvl.as[Double], 'prediction.as[Double]).collect()
        val pdata = dataWithClusters.select('per_dem.as[Double], 'total_qtrly_wages.as[Double], 'prediction.as[Double]).collect()
        val cg = ColorGradient(0.0 -> RedARGB, 1.0 -> GreenARGB, 2.0 -> BlueARGB)
        val plot = Plot.simple(ScatterStyle(pdata.map(_._1), pdata.map(_._2), colors = cg(pdata.map(_._3))))
        SwingRenderer(plot, 800, 800, true)
        //Ask how we're supposed to get back the units for our data
        // dataWithClusters.show()
    }
}