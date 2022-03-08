package example

import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object App {

    private val dataset = "data/diabetes_binary_health_indicators_BRFSS2015.csv"

    def main(args: Array[String]) : Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("Project369").setMaster("local[4]")
        val sc = new SparkContext(conf)

        // Resampling
        val hasDiab = sc.textFile(dataset).map (it => {
            it.split(",").map(_.toDouble)
        }).filter(it => it(0) == 1.0)

        val noDiab = sc.textFile(dataset).map (it => {
            it.split(",").map(_.toDouble)
        }).filter(it => it(0) == 0.0)

        val diabCount = hasDiab.count().toInt
        println("Count with diabetes: " + diabCount)
        val noDiabCount = noDiab.count().toInt
        println("Count without diabetes: " + noDiabCount)

        val resampledMinority = sc.parallelize(noDiab.takeSample(false, diabCount, 369)).union(hasDiab)
        val resampledMajority = sc.parallelize(hasDiab.takeSample(true, noDiabCount, 369)).union(noDiab)

        println("50/50 resample using count with diabetes: " + resampledMinority.count())
        println("50/50 resample using count without diabetes: " + resampledMajority.count())

        // Resampling on stroke
        val strokeOfBadLuck = sc.textFile(dataset).map (it => {
            it.split(",").map(_.toDouble)
        }).filter(it => it(6) == 1.0)

        val noStroke = sc.textFile(dataset).map (it => {
            it.split(",").map(_.toDouble)
        }).filter(it => it(6) == 0.0)

        val strokeCount = strokeOfBadLuck.count().toInt
        println("Count with stroke: " + strokeCount)
        val noStrokeCount = noStroke.count().toInt
        println("Count without stroke: " + noStrokeCount)

        val resampledStrokeMin = sc.parallelize(noStroke.takeSample(false, strokeCount, 369)).union(strokeOfBadLuck)
        val resampledStrokeMaj = sc.parallelize(strokeOfBadLuck.takeSample(true, noStrokeCount, 369)).union(noStroke)

        println("50/50 resample using count with stroke: " + resampledStrokeMin.count())
        println("50/50 resample using count without stroke: " + resampledStrokeMaj.count())
    }

}
