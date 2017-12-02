import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RandomForestImpl {

  def getCSVLines(sc: SparkContext, fileName: String,sep:String): RDD[Array[String]] = {
    val fileRDD = sc.textFile(fileName)
    val header = fileRDD.first()
    fileRDD
      .filter(row => row != header)
      .map(row => row.split(sep))
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("RandomForestRegressionExample")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
//
//    val data = sc.textFile("input/all_features4.csv").map(line => line.split(","))
    val data = getCSVLines(sc, "input/all_features4.csv", ",")
//    val labeledPoints = data.map(x => LabeledPoint(if (x.last == 4) 1 else 0,
//      Vectors.dense(x)))
//    val data = MLUtils.loadLibSVMFile(sc, "input/output")

    val dataPoints = data.map(row =>
      new LabeledPoint(
        row.last.toDouble,
//        Vectors.dense((row(12)toDouble), row(13).toDouble)
        Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))
      )
    ).cache()
    dataPoints.take(5).foreach(x => println(x))



     //Split the data into training and test sets (30% held out for testing)
    val splits = dataPoints.randomSplit(Array(0.7, 0.3), seed = 12345)
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 20 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 20
    val maxBins = 100
    val seed = 1234

    val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    labelsAndPredictions.take(5).foreach(x => println(x))

    val testMSE = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression forest model:\n" + model.toDebugString)

    val predictionAndLabels = labelsAndPredictions.map{case (s, c) => (c, s)}.cache()


    // Instantiate metrics object
    val metrics = new RegressionMetrics(predictionAndLabels)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    println(s"RMSE = ${metrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metrics.r2}")

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")

    // Save and load model
    model.save(sc, "target/tmp/myRandomForestRegressionModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
//    sameModel.predict()
    // $example off$

    sc.stop()
  }
}
