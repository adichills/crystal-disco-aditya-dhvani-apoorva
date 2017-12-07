import java.io._
import java.util.zip.GZIPInputStream

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Model {

  private val log: Logger = Logger.getLogger(Model.getClass)

  def getCSVLines(sc: SparkContext, fileName: String,sep:String): RDD[Array[String]] = {
    val fileRDD = sc.textFile(fileName)
    val header = fileRDD.first()
    fileRDD
      .filter(row => row != header)
      .map(row => row.split(sep))
  }

  def getRowRDD(inputRDD: RDD[String], sep:String): RDD[Row] = {
    val header = inputRDD.first()
    inputRDD
      .filter(row => row != header)
      .map(_.split(sep))
      .map(attributes => Row(attributes(0).trim, attributes(1).trim, attributes(2).trim, attributes(3).trim,
        attributes(4).trim, attributes(5).trim, attributes(6).trim, attributes(7).trim, attributes(8).trim))
  }

  def getArrayStringRDD(inputRDD: RDD[String], sep:String): RDD[Array[String]] = {
    val header = inputRDD.first()
    inputRDD
      .filter(row => row != header)
      .map(row => row.split(sep))
  }

  def getRow(spark: SparkSession, inputRDD: RDD[String]): DataFrame = {
    val customSchema = StructType(
      Array(
        StructField("artist_name",StringType,true),
        StructField("title",StringType,true),
        StructField("song_hotness",StringType,true),
        StructField("artist_hotness",StringType,true),
        StructField("tempo",StringType,true),
        StructField("play_count",StringType,true),
        StructField("similar_artist_count",StringType,true),
        StructField("td_meanPrice",StringType,true),
        StructField("td_confidence",StringType,true)
      )
    )

    spark.sqlContext.createDataFrame(getRowRDD(inputRDD, ","), customSchema)
//    data_df.createOrReplaceTempView("features")
//    val query = "select song_hotness,artist_hotness,tempo,play_count,similar_artist_count,td_meanPrice,td_confidence " +
//      "from features where artist_name = 'downhere' and title = '1000milesapartuncutdemo'"
//    val q = spark.sql(query)
//    q.show()
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

  def cleanString(s:String) :String ={
    s.replaceAll("[^A-Za-z0-9]","").toLowerCase()
  }

//  def getAverage(df: DataFrame, inputColumnName: String): Unit = {
//    df.select(avg($"avbn"))
//  }


  def main(args: Array[String]) {

    var startOverall = System.nanoTime()
    var start = -1L

//    val inputDir = args(0)
//    val outputDir = args(1)

    val bm = new ListBuffer[Array[String]]()
    val bmHeader = Array("impl", "step", "timeInMs")

    val writeMetrics = new ListBuffer[Array[String]]()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("RandomForestRegressionExample")

    if (conf.get("master", "") == "") {
      log.info("Setting master internally")
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    val stream = this.getClass.getResourceAsStream("features.csv.gz")
//    val lines = new GZIPInputStream(stream)
    val lines = Source.fromInputStream(new GZIPInputStream(stream)).getLines()
    val data = sc.parallelize(lines.toList)
//    data.take(5).foreach(x => println(x))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    // input - lower case, strip all alphanumeric characters - for both

    val data_df = getRow(spark, data)


    val featuresRDD = getArrayStringRDD(data, ",")
//    featuresRDD.take(5).foreach(x => println(x))

    // convert to labelled point and pass it to model

//    var song_hotness_avg = getAverage(featuresRDD, 2)
//    println(song_hotness_avg)

    var song_hotness_avg = data_df.select(avg($"song_hotness"))
    println(song_hotness_avg)
    var artist_hotness_avg = data_df.select(avg($"artist_hotness"))
    println(artist_hotness_avg)
    var meanPrice_avg = data_df.select(avg($"td_meanPrice"))
    println(meanPrice_avg)
    var confidence_default = 3.0

//    var playCountCount = data_df.select(count($"play_count")).show()

//    var sortPlayCount = data_df.select(sort_array($"play_count")).toJavaRDD


    var inputBuffer = new ListBuffer[Array[String]]()
    println("Please enter the input, enter quit when done")
    var ln = scala.io.StdIn.readLine()
//    var input = ""
    while (ln != "quit") {
      ln = scala.io.StdIn.readLine()
//      input += ln + "\n"
      inputBuffer.append(Array(ln))
    }
    println(inputBuffer)

    for(i <- 0 to inputBuffer.length-1){

    }

    val tempDirPath : Option[String] = util.Properties.envOrNone("TEMP_DIR_PATH")

    var artistName = cleanString("William Shatner")
    println(artistName)
    var songTitle = cleanString("Ideal Woman")
    println(songTitle)
    var featureBuffer = new ListBuffer[Array[String]]()

    var featureRow = featuresRDD.filter(row => row(0).equals(artistName) && row(1).equals(songTitle))
      .map(row => (row(2), row(3), row(7), row(8)))
    var featureRowCount = featureRow.count()

    featureRow.take(1).foreach(x => println(x))

    if (featureRowCount == 1) {
      featureBuffer.append(Array(featureRow.toString()))
    } else if (featureRowCount == 0) {
      // filter only artist name
      featureRow = featuresRDD.filter(row => row(0).equals(artistName))
        .map(row => (row(2), row(3), row(7), row(8)))
      featureRowCount = featureRow.count()
      if (featureRowCount == 1) {
        featureBuffer.append(Array(featureRow.toString()))
      } else if (featureRowCount != 0) {
        val featureRowDF = featureRow.toDF("song_hotness", "artist_hotness", "td_meanPrice", "td_confidence")
        var song_hotness_value = featureRowDF.select(avg($"song_hotness")).toString()
        var artist_hotness_value = featureRowDF.select(avg($"artist_hotness")).toString()
        var meanPrice_value = featureRowDF.select(avg($"meanPrice")).toString()
        featureBuffer.append(Array(song_hotness_value, artist_hotness_value, meanPrice_value, confidence_default.toString))
      } else {
        // filter only on song title
        featureRow = featuresRDD.filter(row => row(1).equals(songTitle))
          .map(row => (row(2), row(3), row(7), row(8)))
        featureRowCount = featureRow.count()
        if (featureRowCount == 1) {
          featureBuffer.append(Array(featureRow.toString()))
        } else if (featureRowCount != 0) {
          val featureRowDF = featureRow.toDF("song_hotness", "artist_hotness", "td_meanPrice", "td_confidence")
          var song_hotness_value = featureRowDF.select(avg($"song_hotness")).toString()
          var artist_hotness_value = featureRowDF.select(avg($"artist_hotness")).toString()
          var meanPrice_value = featureRowDF.select(avg($"meanPrice")).toString()
          featureBuffer.append(Array(song_hotness_value, artist_hotness_value, meanPrice_value, confidence_default.toString))
        } else {
          featureBuffer.append(Array(song_hotness_avg.toString(), artist_hotness_avg.toString(), meanPrice_avg.toString(), confidence_default.toString))
        }
      }
    } else {
      featureRow = featuresRDD.filter(row => row(0).equals(artistName))
        .map(row => (row(2), row(3), row(7), row(8)))
      val featureRowDF = featureRow.toDF("song_hotness", "artist_hotness", "td_meanPrice", "td_confidence")
      var song_hotness_value = featureRowDF.select(avg($"song_hotness")).toString()
      var artist_hotness_value = featureRowDF.select(avg($"artist_hotness")).toString()
      var meanPrice_value = featureRowDF.select(avg($"meanPrice")).toString()
      featureBuffer.append(Array(song_hotness_value, artist_hotness_value, meanPrice_value, confidence_default.toString))
    }




    val featureData = sc.parallelize(featureBuffer)
    println("dhvani")
    featureData.take(5).foreach(x => println(x))

//    val x = featureData.map(row => (row(3).toDouble, row(0).toDouble, row(1).toDouble, row(2).toDouble))
//    x.take(5).foreach(y => println(y))

//    val printData = featureData.map(row => (
//      println(row(0).toDouble),
//      println(row(1).toDouble),
//      println(row(2).toDouble),
//      println(row(3).toDouble)
//    ))

    val testingData = featureData.map(row => {
        Vectors.dense(row(3).toDouble, row(0).toDouble, row(1).toDouble, row(2).toDouble)
      }
    )

    val model = RandomForestModel.load(sc, args(0))

    val prediction = model.predict(testingData)
    prediction.take(5).foreach(x => println(x))

    // get the output. convert to integer and print to standard output
    sc.stop()
  }
}
