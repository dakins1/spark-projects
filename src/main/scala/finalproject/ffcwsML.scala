package finalproject

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

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.catalyst.expressions.In
import scala.annotation.meta.param
import scalafx.scene.input.KeyCode.D
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.catalyst.expressions.aggregate.Corr
import _root_.scalafx.scene.effect.BlendMode.Green

object FFCWS {
	def main(args:Array[String]):Unit = {
    
  	val spark = SparkSession.builder().master(/*"spark://pandora00:7077"*/"local[*]").appName("Clustering").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

		val data = spark.read.
			option("header", "true").
		csv("C:/Users/Dillon/comp/datasets/FFCWS/outputCSV")
		
		// data.show()
		println("Column count: " + data.columns.size)
		val cols = data.columns
		
		println("Row count: " + data.count())

		/* How many columns have a given percentage of null values? */

		val arr = data.columns.take(5).map(name => col(name))
		val x = data.select(arr:_*)
		
		// val k6Data = data.select('k6d61g, 'k6d62h, 'k6e10, 'k6e12,'k6e25, 'k6e30, 'k6e35a, 'k6e36, 'k6e37)
		// println("selected count: " + k6Data.count())
		// val counts = k6Data.summary("count").first()
		// val totalRows = data.count()
		// val noNulls = (for (i <- 1 to counts.length-1) yield if (counts.get(i).toString.toInt > totalRows / 2) 1 else 0).sum
		// println(noNulls)

		/**** Income Data ****/
		val incomeFunc = udf((fhhinc:Double, mhhinc:Double) => {
			if (mhhinc <= -1.0 && fhhinc <= -1.0) -1
			else if (mhhinc <= -1.0 && fhhinc >= 0.0) fhhinc
			else if (mhhinc >= 0.0 && fhhinc <= -1.0) mhhinc
			else fhhinc + mhhinc
		})
		val pcgIncomeFunc = udf((pcghhinc:Double) => {
			if (pcghhinc <= -1.0) -1.0 else pcghhinc
		})

		val finalIncomeFunc = udf((i1:Double, i2:Double, i3:Double, i4:Double, i5:Double, i6:Double) => {
			val a = Array(i1,i2,i3,i4,i5,i6).filter(_ != -1.0)
			a.sum / a.length
		})

		val avgs = data.
			withColumn("i1", incomeFunc('cf1hhinc, 'cm1hhinc)).
			withColumn("i2", incomeFunc('cf2hhinc, 'cm2hhinc)).
			withColumn("i3", incomeFunc('cf3hhinc, 'cm3hhinc)).
			withColumn("i4", incomeFunc('cf4hhinc, 'cm4hhinc)).
			withColumn("i5", incomeFunc('cf5hhinc, 'cm5hhinc)).
			withColumn("i6", pcgIncomeFunc('cp6hhinc)).
			withColumn("avgInc", finalIncomeFunc('i1,'i2,'i3,'i4,'i5,'i6))
		val incomeData = avgs.select('idnum, 'avgInc)
		// incomeData.show()

		/**** Environmental Instability ****/
		val jobs1Cols = List("m3k22", "m3k23", "f3k23", "f3k23a", "m4k22", "m4k23", "f4k22", "f4k23")

		val jobs1Calc = udf((m3jobs:Double, m3jobsRange:Double, f3jobs:Double, f3jobsRange:Double,
			m4jobs:Double, m4jobsRange:Double, f4jobs:Double, f4jobsRange:Double) => {
			var score = 0.0
			val rangeMap = Map(1.0 -> 1.0, 2.0->4.0, 3.0->8.0, 4.0->15.0, 5.0->21.0)
			val params = Array(m3jobs:Double, m3jobsRange:Double, f3jobs:Double, f3jobsRange:Double,
			m4jobs:Double, m4jobsRange:Double, f4jobs:Double, f4jobsRange:Double)

			for (i <- 0 until params.length by 2) yield {
				if (params(i) <= -1.0) {
					if (params(i+1) > -1.0) {
						score += rangeMap(params(i+1))
					} else if (params(i+1) >= -3.0) score += 5 //if they don't know or refuse. 
				} else score += params(i)
			}

			if (params.count(_ <= -8.0) == params.size) 5 //gotta have some penalty for not being at any survey
			else score
		})
		val ed1 = data.select("idnum", (jobs1Cols):_*)
			.withColumn("jobs1", jobs1Calc($"m3k22", $"m3k23", $"f3k23", $"f3k23a", $"m4k22", $"m4k23", $"f4k22", $"f4k23"))
			

		val jobs2Cols = List("m5i22", "m5i23", "f5i22", "f5i23", "p6k34", "p6k35")
		val jobs2Calc = udf((m5jobs:Double, m5range:Double, f5jobs:Double, f5range:Double, p6jobs:Double, p6range:Double) => {
			var score = 0.0
			val rangeMap = Map(1.0 -> 1.0, 2.0->4.0, 3.0->8.0, 4.0->15.0, 5.0->21.0)
			val params = Array(m5jobs:Double, m5range:Double, f5jobs:Double, f5range:Double, p6jobs:Double, p6range:Double)

			for (i <- 0 until params.length by 2) yield {
				if (params(i) <= -1.0) {
					if (params(i+1) > -1.0) {
						score += rangeMap(params(i+1))
					} else if (params(i+1) >= -3.0) score += 5 //if they don't know or refuse. 
				} else score += params(i)
			}

			if (params.count(_ <= -8.0) == params.size) 5 //gotta have some penalty for not being at any survey
			else score
		})
		val ed2 = data.select("idnum", (jobs2Cols):_*)
			.withColumn("jobs2", jobs2Calc($"m5i22", $"m5i23", $"f5i22", $"f5i23", $"p6k34", $"p6k35"))

		val partnerCols = List("cf2cohp", "cf3cohp", "cf4cohp", "cf5cohp", "cm2cohp", "cm3cohp", "cm4cohp", "cm5cohp", "cp6pcohp")
		val partnerCalc = udf((f2:Double, f3:Double, f4:Double, f5:Double, m2:Double, m3:Double, m4:Double, m5:Double, p6:Double) => {
			var score = 0.0
			val params = Array(f2:Double, f3:Double, f4:Double, f5:Double, m2:Double, m3:Double, m4:Double, m5:Double, p6:Double)
			score += (for (i <- params) yield {
				if (i == -1.0 || i == -2.0 || i == -4.0) 1.0
				else if (i <= -1.0) 0.0
				else i
			}).sum

			if (params.count(_ <= -8.0) == params.size) 5 //gotta have some penalty for not being at any survey
			else score
		})
		val ed3 = data.select("idnum", partnerCols:_*)
			.withColumn("partnerCalc", 
				partnerCalc($"cf2cohp", $"cf3cohp", $"cf4cohp", $"cf5cohp", $"cm2cohp", $"cm3cohp", $"cm4cohp", $"cm5cohp", $"cp6pcohp"))


		val movedCols = List("f2h1a", "f3a10n", "f5f1a", "f4i1a", "m2h1a", "m3a10n", "m5f1a", "m4i1a", "p6j2")
		//-6 also means not moved
		val movedCalc = udf((f2:Double, f3:Double, f5:Double, f4:Double, m2:Double, m3:Double, m5:Double, m4:Double, p6:Double) => {
			var score = 0.0
			val params = Array(f2:Double, f3:Double, f5:Double, f4:Double, m2:Double, m3:Double, m5:Double, m4:Double, p6:Double)

			score += (for (i <- params) yield {
				if (i == -6.0) 0.0
				else if (i == -1.0 || i == -2.0 || i == -4.0) 1.0
				else if (i <= -1.0) 0.0
				else i
			}).sum
			if (params.count(_ <= -8.0) == params.size) 5 //absensce penalty
			else score
		})
		val ed4 = data.select("idnum", movedCols:_*)
			.withColumn("movedCalc", movedCalc($"f2h1a", $"f3a10n", $"f5f1a", $"f4i1a", $"m2h1a", $"m3a10n", $"m5f1a", $"m4i1a", $"p6j2"))

		println("***** Environmental Data ****")

		/**** Parental Sensitivity ****/
		//Higher is worse, lower is better
		//leaving out k5a3a b/c 10 argument limit to UDFs, and guessing that variable isn't as important
		val parentalCols = Array("k5a2a", "k5a2d", "k5a3d", "k6c9f", "k5a2e", "k5a3e",
			"k6c17", "k6c28", "k5a2c", "k5a3c")
		
		val parentCalc = udf((mtalk:Double, mevents:Double, devents:Double, pevents:Double,
			mclose:Double, dclose:Double, m6close:Double, d6close:Double, mtime:Double, dtime:Double) => {
			var score = 0.0
			val arr = Array(mtalk:Double, mevents:Double, devents:Double, pevents:Double,
			mclose:Double, dclose:Double, m6close:Double, d6close:Double, mtime:Double, dtime:Double)
			
			if (mtalk > -1.0) score += (3 - mtalk) 
			
			if (mevents > -1.0) score += mevents //scale is reversed, therefore 3 - x 
			if (devents > -1.0) score += devents
			//p has to be treated differently
			if (pevents > -1.0) score += pevents 
			
			if (mclose > -1.0) score += mclose - 1 //again, scale reversed and goes up to 4
			if (dclose > -1.0) score += dclose - 1
			if (m6close > -1.0) score += m6close - 1
			if (d6close > -1.0) score += d6close - 1

			if (mtime > -1.0) score += (3 - mtime)
			if (dtime > -1.0) score += (3 - dtime)
			 
			if (arr.count(_ == -9.0) == arr.size) 5.0
			else score			
		})

		val pd = data.select("idnum", parentalCols:_*).withColumn("parentCalc", parentCalc(
			$"k5a2a", $"k5a2d", $"k5a3d", $"k6c9f", $"k5a2e", $"k5a3e",
			$"k6c17", $"k6c28", $"k5a2c", $"k5a3c"
		))
		val parentalData = pd.select("idnum", "parentCalc")
		println("*** Parental Data ***")
		// pd.show()
		println("Lost " + (pd.count() - parentalData.count()) + " rows from parents")

		/**** Behavior Rating ****/
		val behaviorCalc = udf((steal50:Double, stealLess50:Double, stoppedByPolice:Double, numStopped:Double, convicted:Double,
			juvy:Double, marijuana:Double, illegalDrug:Double, prescription:Double) => {
			var score = 0.0
			val arr = Array(steal50:Double, stealLess50:Double, stoppedByPolice:Double, numStopped:Double, convicted:Double,
			juvy:Double, marijuana:Double, illegalDrug:Double, prescription:Double)
			if (arr.contains(-9.0)) -1.0 //if anything is -9, they're all -9
			else {
				if (steal50 == -1.0 || steal50 == -2.0 || steal50 == -4.0) score += 1 //refuse or don't know
				else if (steal50 > 1.0) score += steal50 - 1.0 //1.0 is never, therefore subtract 1.0 

				if (stealLess50 == -1.0 || stealLess50 == -2.0 || stealLess50 == -4.0) score += 1 //refuse or don't know
				else if (stealLess50 > 1.0) score += (stealLess50 *.5) - 1.0 //Half the weight for < $50...lol

				if (stoppedByPolice == -1.0 || stoppedByPolice == -2.0 || stoppedByPolice == -4.0) score += 1
				else if (stoppedByPolice == 1.0) {
					if (numStopped == -1.0 || numStopped == -2.0 || numStopped == -4.0) score += 1
					else if (numStopped > 1.0) score += numStopped 
				}

				if (convicted == -1.0 || convicted == -2.0 || convicted == -4.0) score += 1
				else if (convicted == 1.0) score += 3.0 //kind of guessing on weight

				if (juvy == -1.0 || juvy == -2.0 || juvy == -4.0) score += 1
				else if (juvy == 1.0) score += 3.0 //kind of guessing on weight

				if (marijuana == -1.0 || marijuana == -2.0 || marijuana == -4.0) score += 1
				else if (marijuana > 1.0) score += marijuana - 1.0

				if (illegalDrug == -1.0 || illegalDrug == -2.0 || illegalDrug == -4.0) score += 1
				else if (illegalDrug > 1.0) score += illegalDrug - 1.0

				if (prescription == -1.0 || prescription == -2.0 || prescription == -4.0) score += 1
				else if (prescription > 1.0) score += prescription - 1.0
				
				score
			}
		})
		val score = data.withColumn("bhvrlCalc", behaviorCalc(
			'k6d61g, 'k6d61k, 'k6e10, 'k6e12, 'k6e39, 'k6e41a, 'k6f67d, 'k6f73d, 'k6f79d
		))
		val bd = score.select('idnum, 'bhvrlCalc)
		val behavioralData = bd.filter('bhvrlCalc =!= -1.0) //filter rows with no data
		
		val behaviorCols2 = List("k6f26", "k6f43", "k6f63", "k6f65", "k6f68", "k6f71", "k6f74", "k6f77")
		val behavioralCalc2 = udf((hadSex:Double, sexNum:Double, triedMarij:Double, marijUsage:Double,
			triedIllegal:Double, illegalUsage:Double, triedPrescip:Double, prescipUsage:Double) => {
			var score = 0.0
			val params = Array(hadSex:Double, sexNum:Double, triedMarij:Double, marijUsage:Double,
			triedIllegal:Double, illegalUsage:Double, triedPrescip:Double, prescipUsage:Double)

			if (hadSex == 1.0) score += 1
			else if (hadSex == -1.0) score += 1

			if (sexNum == -1.0 || sexNum == -2.0 || sexNum == -4.0) score += 1
			else if (sexNum >= 1.0) score += sexNum

			if (triedMarij == -1.0) score += 1
			else if (triedMarij == 1.0) score += 1

			if (marijUsage == -1.0) score += 1
			else if (marijUsage == -4.0 || marijUsage == -2.0) score += 1
			else if (marijUsage >= 1.0) score += marijUsage

			if (triedIllegal == -1.0) score += 1
			else if (triedIllegal == 1.0) score += 1

			if (illegalUsage == -1.0) score += 1
			else if (illegalUsage == -4.0 || illegalUsage == -2.0) score += 1
			else if (illegalUsage >= 1.0) score += illegalUsage

			if (triedPrescip == -1.0) score += 1
			else if (triedPrescip == 1.0) score += 1

			if (prescipUsage == -1.0) score += 1
			else if (prescipUsage == -4.0 || prescipUsage == -2.0) score += 1
			else if (prescipUsage >= 1.0) score += prescipUsage

			if (params.count(_ == -9.0) == params.size) -1
			else score
		})
		val bd2 = data.select("idnum", behaviorCols2:_*)
			.withColumn("bhvrlCalc2", behavioralCalc2($"k6f26", $"k6f43", $"k6f63", $"k6f65", $"k6f68", $"k6f71", $"k6f74", $"k6f77"))
			.filter('bhvrlCalc2 =!= -1.0)

		println("*** Behavioral Data ***")
		// behavioralData.show()
		println("Lost " + (bd.count() - behavioralData.count()) + " rows from missing data")

		println("********* Master Data ***********")

		val envTotal = udf((jobs1:Double, jobs2:Double, partner:Double, moved:Double) => {
			Array(jobs1:Double, jobs2:Double, partner:Double, moved:Double).sum
		})

		val finalParams = List("avgInc", "parentCalc", "envCalc", "bhvrlCalcTotal")
		val finalCols = List($"idnum".as[Int], $"avgInc".as[Double], 
			$"parentCalc".as[Double], $"envCalc".as[Double], $"bhvrlCalcTotal".as[Double])

		val master = ed1.join(ed2, "idnum").join(ed3, "idnum").join(ed4, "idnum")
			.join(behavioralData, "idnum")
			.join(bd2, "idnum")
			.withColumn("bhvrlCalcTotal", 'bhvrlCalc + 'bhvrlCalc2)
			.join(incomeData, "idnum")
			.join(parentalData, "idnum")
			.withColumn("envCalc", envTotal($"jobs1", $"jobs2", $"partnerCalc", $"movedCalc"))
			.select(finalCols:_*)
		master.show
		master.summary().show()

		val pdata = master.select('avgInc.as[Double], 'parentCalc.as[Double], 'envCalc.as[Double], 'bhvrlCalcTotal.as[Double]).collect()
		val p = Plot.stacked(Seq(
			ScatterStyle(pdata.map(_._1), pdata.map(_._4), symbolWidth=4, symbolHeight=4, colors = pdata.map(_=>GreenARGB)),
			ScatterStyle(pdata.map(_._1), pdata.map(_._4), symbolWidth=4, symbolHeight=4, colors = pdata.map(_=>BlueARGB)),
			ScatterStyle(pdata.map(_._3), pdata.map(_._4), symbolWidth=4, symbolHeight=4, colors = pdata.map(_=>MagentaARGB))
		))
		SwingRenderer(p, 1000, 1000, true)

		val va = new VectorAssembler()
			.setInputCols(finalParams.toArray)
			.setOutputCol("featLilYachty")
		val corrVector = va.transform(master)
		val Row(coeff:Matrix) = Correlation.corr(corrVector, "featLilYachty").head()
		println("Correlation matrix: \n" + coeff)


  }

}