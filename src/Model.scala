//import java.io.{BufferedOutputStream, PrintWriter}
//
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.evaluation.RegressionMetrics
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.tree.RandomForest
//import org.apache.spark.mllib.tree.model.RandomForestModel
//import org.apache.spark.rdd.RDD
//
//import scala.collection.mutable.ListBuffer
//
//object Model {
//
//  private val log: Logger = Logger.getLogger(Model.getClass)
//
//  def getCSVLines(sc: SparkContext, fileName: String,sep:String): RDD[Array[String]] = {
//    val fileRDD = sc.textFile(fileName)
//    val header = fileRDD.first()
//    fileRDD
//      .filter(row => row != header)
//      .map(row => row.split(sep))
//  }
//
//  def toMs(start: Long, end: Long): Long = {
//    (end - start) / (1000L * 1000L)
//  }
//
//  def write(it: Iterable[Iterable[String]], headerIt: Iterable[String], path: Path, sc: SparkContext): Unit = {
//    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
//    val pw: PrintWriter = new PrintWriter(new BufferedOutputStream(fs.create(path)))
//    pw.println(headerIt.mkString(";"))
//    it.foreach(x => pw.println(x.mkString(";")))
//    pw.close
//  }
//
//  def write(it: Iterable[Iterable[String]], path: Path, sc: SparkContext): Unit = {
//    val fs = FileSystem.get(path.toUri, sc.hadoopConfiguration)
//    val pw: PrintWriter = new PrintWriter(new BufferedOutputStream(fs.create(path)))
//    it.foreach(x => pw.println(x.mkString(";")))
//    pw.close
//  }
//
//  def main(args: Array[String]) {
//
//    var startOverall = System.nanoTime()
//    var start = -1L
//
//    val inputDir = args(0)
//    val outputDir = args(1)
//
//    val bm = new ListBuffer[Array[String]]()
//    val bmHeader = Array("impl", "step", "timeInMs")
//
//    val writeMetrics = new ListBuffer[Array[String]]()
//
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
//
//    val conf = new SparkConf().setAppName("RandomForestRegressionExample")
//
//    if (conf.get("master", "") == "") {
//      log.info("Setting master internally")
//      conf.setMaster("local[*]")
//    }
//
//    val sc = new SparkContext(conf)
//
//    // testDataModel is the input
//    val loadModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
//    val loadModelPredict = testDataModel.map { point =>
//      val predictionModel = loadModel.predict(point.features)
//      (point.label, predictionModel)
//    }
//
//    loadModelPredict.saveAsTextFile(outputDir + "/predictedValues")
//
//    val writeMetricsModel = new ListBuffer[Array[String]]()
//    val testMSEModel = loadModelPredict.map { case (v, p) => math.pow((v - p), 2) }.mean()
//    println("Test Mean Squared Error = " + testMSEModel)
//    writeMetricsModel.append(Array("Test Mean Squared Error", testMSEModel.toString()))
//
//    val predictionAndLabelsModel = loadModelPredict.map{case (s, c) => (c, s)}.cache()
//
//    // Instantiate metrics object
//    val metricsModel = new RegressionMetrics(predictionAndLabelsModel)
//
//    println("Data predicted from model")
//    // Squared error
//    println(s"MSE = ${metricsModel.meanSquaredError}")
//    writeMetricsModel.append(Array("MSE", metricsModel.meanSquaredError.toString()))
//    println(s"RMSE = ${metricsModel.rootMeanSquaredError}")
//    writeMetricsModel.append(Array("RMSE", metricsModel.rootMeanSquaredError.toString()))
//
//    // R-squared
//    println(s"R-squared = ${metricsModel.r2}")
//    writeMetricsModel.append(Array("R-squared", metricsModel.r2.toString()))
//
//    // Mean absolute error
//    println(s"MAE = ${metricsModel.meanAbsoluteError}")
//    writeMetricsModel.append(Array("MAE", metricsModel.meanAbsoluteError.toString()))
//
//    // Explained variance
//    println(s"Explained variance = ${metricsModel.explainedVariance}")
//    writeMetricsModel.append(Array("Explained variance", metricsModel.explainedVariance.toString()))
//
//    sc.stop()
//  }
//
//}
