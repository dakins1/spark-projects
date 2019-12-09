
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import scalafx.scene.effect.BlendMode.Red
import io.netty.handler.codec.redis.RedisArrayAggregator
import org.apache.spark.ml.feature.StandardScaler
import breeze.linalg.Matrix

object MLClassification {
    def main(args:Array[String]) {
        val spark =SparkSession
        .builder()
        .master("local[*]")
        .appName("Temp Data")
        .getOrCreate()
        import spark.implicits._

        spark.sparkContext.setLogLevel("WARN")

        val raw = spark.read.option("header", "false").option("inferSchema", "true").option("delimiter", "\t").
        csv("C:/Users/Dillon/comp/datasets/sparksql/admissions/AdmissionAnon.tsv")

        println("Num columns: " + raw.columns.size)
        println("Num rows: " + raw.count())

        println("Distinct values: " + raw.select('_c46).distinct().count())

        raw.groupBy('_c46).count().show()
        raw.agg(count("_c46")).show()

        //did this really cause a fucking error?
        // val Row(coeff1: Matrix) = //unapply pattern match
    }
}