package example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object App {
    // Used in getNeighbors
    private val N = 11

    //private val dataset = "/user/bcordill/input/data"
    private val dataset = "data/5050small.csv"
    //private val dataset = "src/main/sampleData"

    def computeDistance(row1: List[Double], row2: List[Double]): Double = {
        val distance = row1.zip(row2).map({case(x, y) => math.pow(x - y, 2)})
        return math.sqrt(distance.sum)
    }

    def getNeighbors(train: RDD[(Long, (Double, List[Double]))], test: RDD[(Long, (Double, List[Double]))]) : RDD[(Long, Double)] ={
        val data = test.cartesian(train).map({case((i1, (tar1, l1)), (i2, (tar2, l2))) =>
            (i1, (i2, computeDistance(l1, l2), tar2))
        }).groupByKey().map({case(key, vals) =>
            (key, vals.toList)
        }).sortByKey().mapValues(x =>
            x.sortBy(y => (y._2)).take(N)).mapValues(x => x.map({case(i2, dist, tar2) =>
            tar2})).mapValues(x =>
            x.groupBy(identity).mapValues(_.size).maxBy(_._2)._1)
        data

    }

    def main(args: Array[String]) : Unit = {
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf().setAppName("Project369").setMaster("local[4]")
        val sc = new SparkContext(conf)

        printProbabilities(sc)

        val first = sc.textFile(dataset).first()

        // RDD Setup
        val target = sc.textFile(dataset).filter(_ != first).map(line => line.split(",")(0).trim.toDouble)
        val testDiabetes = sc.textFile(dataset).filter(_ != first).map(line => {
            line.split(",").tail.map(item => item.trim.toDouble).toList
        })


        // create list of tuples
        // sample row: (0,(0.0,List(1.0, 0.0, 1.0, 26.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 3.0, 5.0, 30.0, 0.0, 1.0, 4.0, 6.0, 8.0)))
        val data = target.zip(testDiabetes).zipWithIndex().map({case(x, y) => (y, x)})

        // split data into 80% training and 20% testing
        val (training, test) = data.randomSplit(Array[Double](0.8, 0.2), 369) match {
            case Array(training, test) => (training, test)
        }

        // print out what data looks like before k nearest neighbors
        println("Example data to be split into training and testing")
        data.take(5).foreach(println)


        // predicting whether they have diabetes based on other columns
        val actual = target.zipWithIndex().map(x => (x._2, x._1))
        val predicted = getNeighbors(training, test)

        // print accuracy of k nearest neighbors
        println(actual.join(predicted).filter({case (id, (act, pred)) => act == pred}).count.toDouble / test.count.toDouble)


        // print accuracy of k nearest neighbors on all columns
        println()
        println("Accuracy using all factors: " + actual.join(predicted).filter({case (id, (act, pred)) => act == pred}).count.toDouble / test.count.toDouble)

        // try on healthcare, sex, age, income, education (socioeconomic factors)
        val non_health_factors = data.map({case(idx, (tar, l)) => (idx, (tar, List(l(11), l(17), l(18), l(19), l(20))))})
        val (training2, test2) = non_health_factors.randomSplit(Array[Double](0.8, 0.2), 369) match {
          case Array(training2, test2) => (training2, test2)
        }
        val actual2 = target.zipWithIndex().map(x => (x._2, x._1))
        val predicted2 = getNeighbors(training2, test2)
        println()
        println("Accuracy using age, sex, income, healthcare (non health related factors): " + actual2.join(predicted2).filter({case (id, (act, pred)) => act == pred}).count.toDouble / test2.count.toDouble)

        // try on health related factors only
        // high bp, high chol, bmi, stroke, heart disease
        val health_factors = data.map({case(idx, (tar, l)) => (idx, (tar, List(l(0), l(1), l(3), l(5), l(6))))})
        val (training3, test3) = health_factors.randomSplit(Array[Double](0.8, 0.2), 369) match {
          case Array(training3, test3) => (training3, test3)
        }
        val actual3 = target.zipWithIndex().map(x => (x._2, x._1))
        val predicted3 = getNeighbors(training3, test3)
        println()
        println("Accuracy using high bp, high chol, bmi, stroke, heart disease: " + actual3.join(predicted3).filter({case (id, (act, pred)) => act == pred}).count.toDouble / test3.count.toDouble)

        // try on significant factors determined by preliminary analysis
        val sig_factors = data.map({case(idx, (tar, l)) => (idx, (tar, List(l(20), l(1), l(2), l(3), l(4), l(5), l(7), l(9))))})
        val (training4, test4) = sig_factors.randomSplit(Array[Double](0.8, 0.2), 369) match {
          case Array(training4, test4) => (training4, test4)
        }
        val actual4 = target.zipWithIndex().map(x => (x._2, x._1))
        val predicted4 = getNeighbors(training4, test4)
        println()
        println("Accuracy using significant factors from preliminary analysis: " + actual4.join(predicted4).filter({case (id, (act, pred)) => act == pred}).count.toDouble / test4.count.toDouble)
    }

    def printProbabilities(sc: SparkContext): Unit = {
        // 0 diabetes, 1 highBP, 2 highChol, 3 cholCheck, 4 BMI, 5 Smoker, 6 Stroke, 7 heartDiseaseOrAttack, 8 PhysActivity,
        // 9 fruits, 10 veggies, 11 hvyAlcConsum, 12 healthcare, 13 noDocbcCost, 14 genHlth, 15 menHlth, 16 physHlth,
        // 17 diffWalk, 18 Sex, 19 Age, 20 Education, 21 income

        println("High blood pressure on Diabetes Prevalence")
        calculateProbabilities(sc, 1)

        println("High cholesterol on Diabetes Prevalence")
        calculateProbabilities(sc, 2)

        println("BMI on Diabetes Prevalence")
        calculateProbabilities(sc, 4)

        println("Smoking Impact on Diabetes Prevalence")
        calculateProbabilities(sc, 5)

        println("Physical activity on Diabetes Prevalence")
        calculateProbabilities(sc, 8)

        println("Fruit Consumption Impact on Diabetes Prevalence")
        calculateProbabilities(sc, 9)

        println("Vegetable Consumption Impact on Diabetes Prevalence")
        calculateProbabilities(sc, 10)

        println("Heavy alcohol consumption on Diabetes Prevalence")
        calculateProbabilities(sc, 11)

        println("Available healthcare on Diabetes Prevalence")
        calculateProbabilities(sc, 12)

        println("General health 1-5 on Diabetes Prevalence")
        calculateProbabilities(sc, 14)

        println("Difficulty walking on Diabetes Prevalence")
        calculateProbabilities(sc, 17)

        println("Sex on Diabetes Prevalence")
        calculateProbabilities(sc, 18)

        println("Age on Diabetes Prevalence")
        calculateProbabilities(sc, 19)

        println("Education on Diabetes Prevalence")
        calculateProbabilities(sc, 20)

        println("Income on Diabetes Prevalence")
        calculateProbabilities(sc, 21)
    }

  def calculateProbabilities(sc: SparkContext, factor: Int): Unit = {
    val res = sc.textFile("data/diabetes_binary_5050split_health_indicators_BRFSS2015.csv")
      .map(line => (line.split(",")(factor).toDouble, line.split(",")(0).toDouble))
      .combineByKey(v => (v, 1),
        (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
        (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .map({ case (key, value) => (key, value._1 / value._2) })
      .map(line => (line._1, (math floor line._2 * 1000) / 1000)).collect().sortBy(_._1).foreach(line => println(line._1 + ": " + line._2 + " prevalence of diabetes"))

      println()
  }

}
