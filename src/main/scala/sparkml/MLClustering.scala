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

        
        val spark = SparkSession.builder().master(/*"spark://pandora00:7077"*/"local[*]").appName("Clustering").getOrCreate()
        import spark.implicits._
        spark.sparkContext.setLogLevel("WARN")

        val dataQcew = spark.read.
        option("header", "true").
        option("inferSchema", true).
        // csv("/data/BigData/bls/qcew/2016.q1-q4.singlefile.csv").
        csv("C:/Users/Dillon/comp/datasets/sparksql/qcew/2016.q1-q4.singlefile.csv").
        filter("total_qtrly_wages is not null and area_fips is not null and industry_code is not null")

        val dataVotes = spark.read.
        option("header", "true").
        option("inferSchema", true).
        // csv("/data/BigData/bls/2016_US_County_Level_Presidential_Results.csv").
        csv("C:/Users/Dillon/comp/datasets/sparksql/2016_US_County_Level_Presidential_Results.csv").
        filter('state_abbr =!= "AK").
        filter(row => !row.anyNull)

        val dataCounty = spark.read.
        option("header", "true").
        option("inferSchema", true).
        // csv("/data/BigData/bls/qcew/area_titles.csv").
        csv("C:/Users/Dillon/comp/datasets/sparksql/qcew/area_titles.csv").
        filter(row => !row.anyNull)

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

        val dataQcewInt = dataQcew.filter(!'area_fips.contains("U") && 'area_fips.substr(3,3) =!= "000").
            withColumn("fips_int", 'area_fips.cast(IntegerType))

        val dataVotesInt = dataVotes.withColumn("fips_int", 'combined_fips.cast(IntegerType))

        val joined = dataVotesInt.join(dataQcewInt, "fips_int").
            select('area_fips, 'combined_fips, 'fips_int, 'industry_code, 'qtr, 'year, 'total_qtrly_wages, 'month2_emplvl, 'qtrly_estabs, 'per_dem)

        //TODO move up to finer clusters, try to add more columns to help with distinctions
        //TODO also make sure to calculate the percentage complete and graph the stuff

        val q4Data = dataQcewInt.filter('qtr === "4").
            groupBy('fips_int).
            agg(avg("taxable_qtrly_wages").as("taxable_qtrly_wages"), avg("month2_emplvl").as("month2_emplvl")).
            join(dataVotesInt, "fips_int").
            select('taxable_qtrly_wages, 'month2_emplvl, 'per_dem)

        val va = new VectorAssembler()
            .setInputCols(Array("taxable_qtrly_wages", "month2_emplvl"))
            .setOutputCol("featRihanna")
        val dataWithFeatures = va.transform(q4Data) 
        
        val scaler = new StandardScaler()
            .setInputCol("featRihanna")
            .setOutputCol("featFuture")
        val scalerModel = scaler.fit(dataWithFeatures)
        val scaledData = scalerModel.transform(dataWithFeatures).cache()
        
        println("Beginning clustering")
        
        val kmeans = new KMeans().setK(4).setFeaturesCol("featFuture")
        val kmeansModel = kmeans.fit(scaledData)
        val dataWithClusters = kmeansModel.transform(scaledData)
        
        dataWithClusters.show()
        dataWithClusters.filter('prediction === 1).show()
        dataWithClusters.orderBy(desc("prediction")).show()

        val crct = dataWithClusters.filter(
            (('prediction === 0 || 'prediction === 1) && 'per_dem < 0.5) || 
            (('prediction === 2 || 'prediction === 3) && 'per_dem >= 0.5)
        )
        // val crct = dataWithClusters.filter(('prediction === 0 && 'per_dem < 0.5) || ('prediction === 1 && 'per_dem >= 0.5))

        println("crct count: " + crct.count())
        println("regular count: " + dataWithClusters.count())
        println("perc. correct: " + crct.count() / dataWithClusters.count().toDouble)

        println("Beginning plotting")
        
        //qtrly wages, month1 emp level, per dem
        val pdata = dataWithClusters.select('per_dem.as[Double], 'taxable_qtrly_wages.as[Double], 'prediction.as[Double]).collect()
        val cg = ColorGradient(0.0 -> RedARGB, 1.0 -> YellowARGB, 2.0 -> MagentaARGB, 3.0 -> BlueARGB))
        val plot = Plot.simple(ScatterStyle(pdata.map(_._1), pdata.map(_._2), colors = cg(pdata.map(_._3)),
            symbolWidth = pdata.map(_ => 5), symbolHeight = pdata.map(_ => 5)), "Clustering", "% Dem", "Taxable qtrly wages")
        SwingRenderer(plot, 800, 800, true) 
    }
}