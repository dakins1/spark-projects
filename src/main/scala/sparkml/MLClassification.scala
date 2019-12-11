
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
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix

object MLClassification {
    def main(args:Array[String]) {
        val spark =SparkSession
        .builder()
        .master("local[*]")
        .appName("Temp Data")
        .getOrCreate()
        import spark.implicits._

        spark.sparkContext.setLogLevel("WARN")

        val data = spark.read.option("header", "false").option("inferSchema", "true").option("delimiter", "\t").
        csv("C:/Users/Dillon/comp/datasets/sparksql/admissions/AdmissionAnon.tsv")

        println("Num columns: " + data.columns.size)
        println("Num rows: " + data.count())

        println("Distinct values: " + data.select('_c46).distinct().count())

        data.groupBy('_c46).count().show()
        data.agg(count("_c46")).show()

        val numericTypes = Array("int", "float", "double", "long", "decimal")
        val numericDataNames = data.schema.fields.filter(x => numericTypes.contains(x.dataType.typeName)).map(_.name)
        val numericData = data.select(numericDataNames.head, numericDataNames.tail:_*).filter(!_.anyNull)

        val va = new VectorAssembler()
            .setInputCols(numericDataNames)
            .setOutputCol("featRihanna")
        val vectData = va.transform(numericData)
        // vectData.select("featRihanna").show()

        val Row(coeff1: Matrix) = Correlation.corr(vectData, "featRihanna").head()
        println("Print the fucking matrix")
        println(coeff1.toString(18, Int.MaxValue))
    }
}