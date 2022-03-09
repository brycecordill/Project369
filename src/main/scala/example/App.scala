package example

import App.getNeighbors
import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level


import java.io._
import scala.collection._

object App {

    private val dataset = "data/diabetes_binary_health_indicators_BRFSS2015.csv"

    def computeDistance(row1: List[Double], row2: List[Double]): Double = {
        val distance = row1.zip(row2).map({case(x, y) => math.pow(x - y, 2)})
        return math.sqrt(distance.sum)
    }

    def getNeighbors(train: RDD[(Long, (Double, List[Double]))], test: RDD[(Long, (Double, List[Double]))]) : RDD[(Long, Double)] ={
        //var allDistances = mutable.MutableList[()]
        val data = test.cartesian(train).map({case((i1, (tar1, l1)), (i2, (tar2, l2))) =>
            (i1, (i2, computeDistance(l1, l2), tar1, tar2))
        }).groupByKey().map({case(key, vals) =>
            (key, vals.toList)
        }).sortByKey().mapValues(x =>
            x.sortBy(y => (y._2)).take(N)).mapValues(x => x.map({case(i2, dist, tar1, tar2) =>
            tar2})).mapValues(x =>
            x.groupBy(identity).mapValues(_.size).maxBy(_._2)._1)
        data

    }

    def main(args: Array[String]) : Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("Project369").setMaster("local[4]")
        val sc = new SparkContext(conf)

        val first = sc.textFile(dataset).first()

        // Resampling
        val hasDiab = sc.textFile(dataset).filter(_ != first)
          .map (it => {
            it.split(",").map(_.toDouble)
        }).filter(it => it(0) == 1.0)

        val noDiab = sc.textFile(dataset).filter(_ != first)
          .map (it => {
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

<<<<<<< HEAD
        val target = sc.textFile("src/main/sampleData").map(line => line.split(",")(0).trim.toDouble)
        val testDiabetes = sc.textFile("src/main/sampleData").map(line => List(
            line.split(",")(1).trim.toDouble,
            line.split(",")(2).trim.toDouble,
            line.split(",")(3).trim.toDouble,
            line.split(",")(4).trim.toDouble,
            line.split(",")(5).trim.toDouble,
            line.split(",")(6).trim.toDouble,
            line.split(",")(7).trim.toDouble,
            line.split(",")(8).trim.toDouble,
            line.split(",")(9).trim.toDouble,
            line.split(",")(10).trim.toDouble,
            line.split(",")(11).trim.toDouble,
            line.split(",")(12).trim.toDouble,
            line.split(",")(13).trim.toDouble,
            line.split(",")(14).trim.toDouble,
            line.split(",")(15).trim.toDouble,
            line.split(",")(16).trim.toDouble,
            line.split(",")(17).trim.toDouble,
            line.split(",")(18).trim.toDouble,
            line.split(",")(19).trim.toDouble,
            line.split(",")(20).trim.toDouble,
            line.split(",")(21).trim.toDouble))

        // create list of tuples
        // sample row: (0,(0.0,List(1.0, 0.0, 1.0, 26.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 3.0, 5.0, 30.0, 0.0, 1.0, 4.0, 6.0, 8.0)))
        val data = target.zip(testDiabetes).zipWithIndex().map({case(x, y) => (y, x)})

        // split data into 80% training and 20% testing
        val (training, test) = data.randomSplit(Array[Double](0.8, 0.2), 369) match {
            case Array(training, test) => (training, test)
        }

        // print out what data looks like before k nearest neighbors
        data.take(5).foreach(println)

        // predicting whether they have diabetes based on other columns
        val actual = target.zipWithIndex().map(x => (x._2, x._1))
        val predicted = getNeighbors(training, test)

        // print accuracy of k nearest neighbors
        println(actual.join(predicted).filter({case (id, (act, pred)) => act == pred}).count.toDouble / test.count.toDouble)


        val pw = new PrintWriter(new File("data/output.csv"))
        pw.write(first + "\n")
        resampledMajority.map(it => it.mkString(",")).collect().foreach(x=> pw.write(x+"\n"))

        // Resampling on stroke
//        val strokeOfBadLuck = sc.textFile(dataset).map (it => {
//            it.split(",").map(_.toDouble)
//        }).filter(it => it(6) == 1.0)
//
//        val noStroke = sc.textFile(dataset).map (it => {
//            it.split(",").map(_.toDouble)
//        }).filter(it => it(6) == 0.0)
//
//        val strokeCount = strokeOfBadLuck.count().toInt
//        println("Count with stroke: " + strokeCount)
//        val noStrokeCount = noStroke.count().toInt
//        println("Count without stroke: " + noStrokeCount)
//
//        val resampledStrokeMin = sc.parallelize(noStroke.takeSample(false, strokeCount, 369)).union(strokeOfBadLuck)
//        val resampledStrokeMaj = sc.parallelize(strokeOfBadLuck.takeSample(true, noStrokeCount, 369)).union(noStroke)
//
//        println("50/50 resample using count with stroke: " + resampledStrokeMin.count())
//        println("50/50 resample using count without stroke: " + resampledStrokeMaj.count())

    }

}
