import java.io.{BufferedOutputStream, PrintWriter}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object RandomForestImpl {

  private val log: Logger = Logger.getLogger(RandomForestImpl.getClass)

  def getCSVLines(sc: SparkContext, fileName: String,sep:String): RDD[Array[String]] = {
    val fileRDD = sc.textFile(fileName)
    val header = fileRDD.first()
    fileRDD
      .filter(row => row != header)
      .map(row => row.split(sep))
  }

  def toMs(start: Long, end: Long): Long = {
    (end - start) / (1000L * 1000L)
  }

  def write(it: Iterable[Iterable[String]], headerIt: Iterable[String], path: Path, sc: SparkContext): Unit = {
    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
    val pw: PrintWriter = new PrintWriter(new BufferedOutputStream(fs.create(path)))
    pw.println(headerIt.mkString(";"))
    it.foreach(x => pw.println(x.mkString(";")))
    pw.close
  }

  def write(it: Iterable[Iterable[String]], path: Path, sc: SparkContext): Unit = {
    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
    val pw: PrintWriter = new PrintWriter(new BufferedOutputStream(fs.create(path)))
    it.foreach(x => pw.println(x.mkString(";")))
    pw.close
  }

  def getNumericalArray(strArr:Array[String],arrDouble:List[Double]): Array[Double] ={
      var listD:ListBuffer[Double] = new ListBuffer[Double]
      for(i<- 0 to strArr.length-1){
        if(arrDouble.contains(i)){
          listD.append(strArr(i).toDouble)
        }
      }
    listD.toArray
  }

  def main(args: Array[String]) {

    var startOverall = System.nanoTime()
    var start = -1L

    val inputDir = args(0)
    val outputDir = args(1)
    val featureInputFile = args(2)
    val testDataFile = args(3)

    val bm = new ListBuffer[Array[String]]()
    val bmHeader = Array("impl", "step", "timeInMs")

    val writeMetrics = new ListBuffer[Array[String]]()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("RandomForestRegressionExample")
    //conf.setMaster("yarn")
//    if (conf.get("master", "") == "") {
//      log.info("Setting master internally")
//      conf.setMaster("local[*]")
//    }

    val sc = new SparkContext(conf)

    val featureInputString = getCSVLines(sc,featureInputFile,",")
    var featuresWanted = featureInputString.map(row => {row.map(x=>x.toDouble)}).take(1)(0).toList

    var featureSet = featuresWanted.toSet


    start = System.nanoTime()
    val data = getCSVLines(sc, inputDir, ",")
//    data.take(5).foreach(x => println(x))

    val dataPoints = data.map(row =>
      new LabeledPoint(
        row(16).toDouble,
        Vectors.dense(getNumericalArray(row,featuresWanted))
      )
    )

    val testData = getCSVLines(sc, testDataFile, ",")
    val testingData = testData.map(row =>
      new LabeledPoint(
        row(16).toDouble,
        Vectors.dense(getNumericalArray(row,featuresWanted))
      )
    )


    bm.append(Array(sc.master, "read data", toMs(start, System.nanoTime()).toString()))

//    dataPoints.take(5).foreach(x => println(x))
//    testingData.take(5).foreach(x => println(x))

//    start = System.nanoTime()
     //Split the data into training and test sets (30% held out for testing)
//    val splits = dataPoints.randomSplit(Array(0.7, 0.3))
//    val (trainingData, testData) = (splits(0), splits(1))
//    bm.append(Array(sc.master, "split data", toMs(start, System.nanoTime()).toString()))

    start = System.nanoTime()
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    //val numClasses = 2

    var categoricalFeaturesInfo = Map[Int, Int]()
    if (featureSet.contains(0.0)) {
      categoricalFeaturesInfo = Map[Int, Int](
        (0, 6)
      )
    }

//    println(categoricalFeaturesInfo)

    val numTrees = args(4).toInt // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = args(5).toInt
    val maxBins = 100
//    val seed = 1234

    val model = RandomForest.trainRegressor(dataPoints, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    bm.append(Array(sc.master, "training data", toMs(start, System.nanoTime()).toString()))

    start = System.nanoTime()
    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    bm.append(Array(sc.master, "testing data", toMs(start, System.nanoTime()).toString()))
//    labelsAndPredictions.take(5).foreach(x => println(x))

    start = System.nanoTime()
    val testMSE = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    writeMetrics.append(Array("Test Mean Squared Error", testMSE.toString()))
//    println("Learned regression forest model:\n" + model.toDebugString)

    val predictionAndLabels = labelsAndPredictions.map{case (s, c) => (c, s)}

    // Instantiate metrics object
    val metrics = new RegressionMetrics(predictionAndLabels)

    // Squared error
    println(s"MSE = ${metrics.meanSquaredError}")
    writeMetrics.append(Array("MSE", metrics.meanSquaredError.toString()))
    println(s"RMSE = ${metrics.rootMeanSquaredError}")
    writeMetrics.append(Array("RMSE", metrics.rootMeanSquaredError.toString()))

    // R-squared
    println(s"R-squared = ${metrics.r2}")
    writeMetrics.append(Array("R-squared", metrics.r2.toString()))

    // Mean absolute error
    println(s"MAE = ${metrics.meanAbsoluteError}")
    writeMetrics.append(Array("MAE", metrics.meanAbsoluteError.toString()))

    // Explained variance
    println(s"Explained variance = ${metrics.explainedVariance}")
    writeMetrics.append(Array("Explained variance", metrics.explainedVariance.toString()))
    bm.append(Array(sc.master, "getting evaluation metrics", toMs(start, System.nanoTime()).toString()))


    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

    start = System.nanoTime()
    // Evaluate model on the training data itself to check if its underfitting or overfitting
    val labelsAndPredictionsTrainData = dataPoints.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    bm.append(Array(sc.master, "running model on training data", toMs(start, System.nanoTime()).toString()))
    //    labelsAndPredictions.take(5).foreach(x => println(x))

    start = System.nanoTime()
    val testMSETD = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Test Mean Squared Error Training Data = " + testMSETD)
    writeMetrics.append(Array("Test Mean Squared Error Training Data", testMSETD.toString()))
    //    println("Learned regression forest model:\n" + model.toDebugString)

    val predictionAndLabelsTrainData = labelsAndPredictionsTrainData.map{case (s, c) => (c, s)}

    // Instantiate metrics object
    val metricsTD = new RegressionMetrics(predictionAndLabelsTrainData)

    // Squared error
    println(s"MSE = ${metricsTD.meanSquaredError}")
    writeMetrics.append(Array("MSE Training Data", metricsTD.meanSquaredError.toString()))
    println(s"RMSE Training Data = ${metricsTD.rootMeanSquaredError}")
    writeMetrics.append(Array("RMSE Training Data", metricsTD.rootMeanSquaredError.toString()))

    // R-squared
    println(s"R-squared Training Data = ${metricsTD.r2}")
    writeMetrics.append(Array("R-squared Training Data", metricsTD.r2.toString()))

    // Mean absolute error
    println(s"MAE Training Data = ${metricsTD.meanAbsoluteError}")
    writeMetrics.append(Array("MAE Training Data", metricsTD.meanAbsoluteError.toString()))

    // Explained variance
    println(s"Explained variance Training Data = ${metricsTD.explainedVariance}")
    writeMetrics.append(Array("Explained variance Training Data", metricsTD.explainedVariance.toString()))
    bm.append(Array(sc.master, "getting evaluation metrics for training data", toMs(start, System.nanoTime()).toString()))


    start = System.nanoTime()
    bm.append(Array(sc.master, "total time", toMs(startOverall, System.nanoTime()).toString()))

    write(bm.map(_.toList), bmHeader.toList, new Path(outputDir, "bm.csv"), sc)
    write(writeMetrics.map(_.toList), new Path(outputDir, "metrics.csv"), sc)

  }
}
