
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
import org.apache.spark.ml.feature.Imputer

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


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

        println(data.schema)
        data.schema.fields.map(_.dataType.typeName).foreach(println)

        data.groupBy('_c46).count().show()
        data.agg(count("_c46")).show()
        
        import org.apache.spark.sql.types._
        val numericTypes = Array("integer", "float", "double", "long", "decimal")
        val numericDataNames = data.schema.fields.filter(x => numericTypes.contains(x.dataType.typeName)).map(_.name)
        def func(column: org.apache.spark.sql.Column) = column.cast(DoubleType)
        val numericColumns = numericDataNames.map(c => col(c).cast(DoubleType))
        val numericData = data.select(numericDataNames.map(name => func(col(name))):_*)

        numericDataNames.foreach(println)
        numericData.show()
        val imputer = new Imputer()
            .setInputCols(numericDataNames)
            .setOutputCols(numericDataNames.map(_ + "i"))
        val impModel = imputer.fit(numericData)
            
        val impNames = numericDataNames.map(_ + "i")
        val impCols = impNames.map(c => col(c))

        val impData = impModel.transform(numericData).select(impCols:_*)

        
        val va = new VectorAssembler()
            .setInputCols(impNames)
            .setOutputCol("featRihanna")
        val vectData = va.transform(impData)

        val Row(coeff1: Matrix) = Correlation.corr(vectData, "featRihanna").head()
    
        println(coeff1.toString(coeff1.numRows, Int.MaxValue))


        val cs = (for (i <- 0 until coeff1.numRows) yield coeff1(coeff1.numCols-1, i)).zip(numericDataNames)
            .filter(_._1 != 1.0).sortBy(_._1.abs).takeRight(3)
        println(cs)
        println(coeff1(0, 2), " ", coeff1(1, 0))

        /* 6. Classification */
        println("Vector data")
        vectData.select("featRihanna").show(false)

        // val randomForest = new RandomForestClassifier()
        //     .setFeaturesCol("featRihanna")
        //     .setLabelCol()



    }
}