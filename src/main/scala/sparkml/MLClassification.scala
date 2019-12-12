
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
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.NaiveBayes


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

        /*
        println("Num columns: " + data.columns.size)
        println("Num rows: " + data.count())

        println("Distinct values: " + data.select('_c46).distinct().count())

        println(data.schema)
        data.schema.fields.map(_.dataType.typeName).foreach(println)

        data.groupBy('_c46).count().show()
        data.agg(count("_c46")).show()
        */
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
        
        // val va = new VectorAssembler()
        //     .setInputCols(impNames)
        //     .setOutputCol("featRihanna")
        // val vectData = va.transform(impData)

        // val Row(coeff1: Matrix) = Correlation.corr(vectData, "featRihanna").head()
    
        // println(coeff1.toString(coeff1.numRows, Int.MaxValue))


        // val cs = (for (i <- 0 until coeff1.numRows) yield coeff1(coeff1.numCols-1, i)).zip(numericDataNames)
        //     .filter(_._1 != 1.0).sortBy(_._1.abs).takeRight(3)
        // println(cs)
        // println(coeff1(0, 2), " ", coeff1(1, 0))

        /* 6. Classification */

        val cva = new VectorAssembler()
            .setInputCols(impNames.dropRight(1))
            .setOutputCol("featDrake")
        val cData = cva.transform(impData).withColumn("binary", when('_c46i===0.0 || '_c46i===1.0, 0.0).otherwise(1.0))



        val splits = cData.randomSplit(Array(.7, .3))
        val (training, testing) = splits(0).cache -> splits(1).cache()


        val randomForest = new RandomForestClassifier()
            .setFeaturesCol("featDrake")
            .setLabelCol("binary")
            .setNumTrees(5)
        val rtModel = randomForest.fit(training)
        val rtData = rtModel.transform(testing)
        val evaluator = new BinaryClassificationEvaluator()
            .setLabelCol("binary")
            // .setPredictionCol("prediction")
            // .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(rtData)
        println("Accuracy = " + accuracy)

        val dTree = new DecisionTreeClassifier()
            .setFeaturesCol("featDrake")
            .setLabelCol("binary")
            .setMaxDepth(5)
        val dTreeModel = dTree.fit(training)
        val dData = dTreeModel.transform(testing)
        val evaluator1 = new BinaryClassificationEvaluator()
            .setLabelCol("binary")
            // .setPredictionCol("prediction")
            // .setMetricName("accuracy")
        val accuracy1 = evaluator1.evaluate(dData)
        println("Accuracy = " + accuracy1)
        
        val keyElems = rtModel.featureImportances
        println(keyElems)
        val keyElems1 = dTreeModel.featureImportances
        println(keyElems1)

        val karr = keyElems1.toArray 
        val top3 = (for (i <- 0 until karr.size) yield (i -> karr(i))).sortBy(-_._2).take(3)
        top3.foreach(d => println(impNames(d._1) -> d._2))

        val nb = new NaiveBayes()
            .setFeaturesCol("featDrake")
            .setLabelCol("_c46i")
        val nbModel = nb.fit(training)
        val nbData = nbModel.transform(cData)
        val newEval = new MulticlassClassificationEvaluator()
            .setLabelCol("_c46i")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        val accuracy2 = newEval.evaluate(nbData)
        println("Accuracy = " + accuracy2)




        


    }
}