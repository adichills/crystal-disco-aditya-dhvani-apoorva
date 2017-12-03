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



    val bm = new ListBuffer[Array[String]]()
    val bmHeader = Array("impl", "step", "timeInMs")

    val writeMetrics = new ListBuffer[Array[String]]()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("RandomForestRegressionExample")
    //conf.setMaster("yarn")
    val sc = new SparkContext(conf)

    val featureInputString = getCSVLines(sc,featureInputFile,",")
    var featuresWanted = featureInputString.map(row => {row.map(x=>x.toDouble)}).take(1)(0).toList

    //if (conf.get("master", "") == "") {
      //log.info("Setting master internally")
      //conf.setMaster("local[*]")
    //}

    start = System.nanoTime()
    val data = getCSVLines(sc, inputDir, ",")

    val dataPoints = data.map(row =>
      new LabeledPoint(
        row.last.toDouble,
        // 0 - artist_hotttnesss, 1 - artist_familiarity, 2 - loudness, 3 - tempo, 4 - duration, 5 - song_hotttnesss,
        // 6 - year, 7 - play_count, 8 - song_count, 9 - weight_dot_freq, 10 - similar_artist_count, 11 - likes,
        // 12 - td_meanPrice, 13 - td_confidence, 14 - top_genere, 15 - td_downloads
//        Vectors.dense((row(0)toDouble), (row(1)toDouble), (row(2)toDouble), (row(3)toDouble), (row(4)toDouble),
//          (row(5)toDouble), (row(6)toDouble), (row(7)toDouble), (row(8)toDouble), (row(9)toDouble), (row(10)toDouble),
//          (row(11)toDouble), (row(12)toDouble), row(13).toDouble, (row(14)toDouble))
         Vectors.dense(getNumericalArray(row,featuresWanted))
//        Vectors.dense(row.take(row.length - 1).map(str => str.toDouble))
      )
    ).cache()
    bm.append(Array(sc.master, "read data", toMs(start, System.nanoTime()).toString()))
    dataPoints.take(5).foreach(x => println(x))

    start = System.nanoTime()
     //Split the data into training and test sets (30% held out for testing)
    val splits = dataPoints.randomSplit(Array(0.7, 0.3), seed = 12345)
    val (trainingData, testData) = (splits(0), splits(1))
    bm.append(Array(sc.master, "split data", toMs(start, System.nanoTime()).toString()))

    start = System.nanoTime()
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
    bm.append(Array(sc.master, "training data", toMs(start, System.nanoTime()).toString()))

    start = System.nanoTime()
    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    bm.append(Array(sc.master, "testing data", toMs(start, System.nanoTime()).toString()))
    labelsAndPredictions.take(5).foreach(x => println(x))

    start = System.nanoTime()
    val testMSE = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    writeMetrics.append(Array("Test Mean Squared Error", testMSE.toString()))
//    println("Learned regression forest model:\n" + model.toDebugString)

    val predictionAndLabels = labelsAndPredictions.map{case (s, c) => (c, s)}.cache()

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

    start = System.nanoTime()
    // Save and load model
    model.save(sc, "target/tmp/myRandomForestRegressionModel")
    bm.append(Array(sc.master, "saving model", toMs(start, System.nanoTime()).toString()))

    bm.append(Array(sc.master, "total time", toMs(startOverall, System.nanoTime()).toString()))
//    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
//    sameModel.predict()
    var empty = ""
    write(bm.map(_.toList), bmHeader.toList, new Path(outputDir, "bm.csv"), sc)
    write(writeMetrics.map(_.toList), new Path(outputDir, "metrics.csv"), sc)

    
  }
}
