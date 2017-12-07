import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.control.Exception.allCatch


object Features {

  def main(args:Array[String]): Unit = {

    //query(args)
    hack()


  }

  def hack(): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    cleanFeatures(spark.sparkContext.textFile("features_small_temp"))

  }



  def genreQuery(): DataFrame ={
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val song_info_df = getCSVIntoDF(spark,"/home/aditya/songs/all/song_info.csv",";")
    song_info_df.printSchema()

    song_info_df.createOrReplaceTempView("song_info")

    val artist_terms_df = getCSVIntoDF(spark,"/home/aditya/songs/all/artist_terms.csv",";")

    artist_terms_df.createOrReplaceTempView("artist_terms")

    val query = "select artist_terms.artist_term,artist_hotttnesss from song_info,artist_terms where artist_terms.artist_id = song_info.artist_id"
    val q = spark.sql(query)
    q.createOrReplaceTempView("term_hotness")

    val query2 = "select artist_term,avg(artist_hotttnesss) as artist_hotness from term_hotness group by artist_term order by artist_hotness desc limit 1000"
    val q2 = spark.sql(query2)
    q2.createOrReplaceTempView("top_20_artist_terms")

    val top_20_terms = spark.sql("select artist_term from top_20_artist_terms").rdd.map(row=>row.toString().replace("[","").replace("]","")).take(1000).toSet


    val customSchema = StructType(
      Array(
        StructField("artistId3",StringType,true),
        StructField("top_50_count",IntegerType,true))
    )

    val artist_top_20_genereRDD = artist_terms_df.
      rdd
      .map(row=>row.toString().replace("[","").replace("]","").split(","))
      .map(row=>(row(0),row(1)))
      .groupByKey()
      .map({case (x,y) => Row(x,intersection_query(y.toSet,top_20_terms))})


    val df = spark.createDataFrame(artist_top_20_genereRDD,customSchema)
    df

  }

  def intersection_query(set:Set[String],top_20_terms:Set[String]):Integer={

    set.intersect(top_20_terms).size
  }

  def get_artist_jams_query(sparkSession:SparkSession): Unit ={
    //val uniqueArtistRDD = getCSVIntoRDD(sparkSession.sparkContext,"/home/aditya/Downloads/MillionSongSubset/AdditionalFiles/unique_artists.txt","<SEP>")
    val uniqueArtistDF = getCSVIntoDF(sparkSession,"/home/aditya/Downloads/MillionSongSubset/AdditionalFiles/unique_artists.txt",",","false");


  }





  def query(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._



    val song_info_df = getCSVIntoDF(spark,"/home/aditya/songs/all/song_info.csv",";")
    song_info_df.printSchema()

    song_info_df.createOrReplaceTempView("song_info")

    val song_info_columns = "select song_id,track_id,artist_id,artist_name,title,artist_hotttnesss,artist_familiarity,loudness,tempo,duration,song_hotttnesss,year"

    val query1 = "select song_id,track_id,artist_id,lower(artist_name) as artist_name,lower(title) as title,artist_hotttnesss,artist_familiarity,loudness,tempo,duration,song_hotttnesss,year from song_info"
    val q1 = spark.sql(query1)
    val cleanedSongInfoDF = cleanSongInfo(q1,spark)
    println(cleanedSongInfoDF.count())
    cleanedSongInfoDF.createOrReplaceTempView("song_info")
    q1.show()
    cleanedSongInfoDF.show()

    spark.sqlContext.cacheTable("song_info")



    val taste_profile_df = getCSVIntoDF(spark,"/home/aditya/downloads_file/play_count.csv",",")
    taste_profile_df.printSchema()
    taste_profile_df.createOrReplaceTempView("taste_profile")
    val query2_part_1 = "select song_id,track_id,artist_id,artist_name,title,artist_hotttnesss,artist_familiarity,loudness,tempo,duration,song_hotttnesss,year,play_count from song_info"
    val query2 = query2_part_1+ " left join taste_profile on song_info.song_id = taste_profile.play_song_id and track_id = taste_profile.play_track_id"
    val q2 = spark.sql(query2)
    //q2.show()
    //combine taste profile data with song info

    println(q2.count)
    //fill null values 0

    val q2temp = q2.na.fill("0",Array("play_count"))
    q2temp.createOrReplaceTempView("song_info_play_count")
    spark.sqlContext.cacheTable("song_info_play_count")


    val q3 = spark.sql("select artist_id as artid ,count(song_id) as song_count from song_info group by artist_id")
    println(q3.count)
    q3.createOrReplaceTempView("artist_songs")

    val q4 = spark.sql(song_info_columns + ", play_count" + ", song_count " + "from song_info_play_count,artist_songs where song_info_play_count.artist_id = artist_songs.artid")

    println(q4.count)
    q4.createOrReplaceTempView("song_info_play_songs_count")
    spark.sqlContext.cacheTable("song_info_play_songs_count")

    val df = getArtistRelatedFeatures1(spark)
    df.printSchema()
    df.createOrReplaceTempView("artist_features_1")

    val df2 = getAristRelatedFeatures2(spark)
    df2.printSchema()
    df2.createOrReplaceTempView("artist_features_2")

    val temp1 = ", play_count" + ", song_count " +", weight_dot_freq "

    val q5 = spark.sql(song_info_columns + temp1 +  "from song_info_play_songs_count left join artist_features_1 on artist_id=artistid1")
    val q5temp = q5.na.fill(0,Array("weight_dot_freq"))
    q5temp.createOrReplaceTempView("song_info_3")
    spark.sqlContext.cacheTable("song_info_3")
    println(q5temp.count())


    val q6 = spark.sql(song_info_columns + temp1 + ", similar_artist_count"+ " from song_info_3 left join artist_features_2 on artist_id=artistId2")
    val q6Temp = q6.na.fill(0,Array("similar_artist_count"))
    q6Temp.createOrReplaceTempView("song_info_4")
    spark.sqlContext.cacheTable("song_info_4")

    println(q6Temp.count)

    val df3 = getCSVIntoDF(spark,"/home/aditya/downloads_file/trackId_likes.csv",",")
    df3.printSchema()
    df3.createOrReplaceTempView("trackLikes")

    val selectColumns = song_info_columns + temp1+", similar_artist_count" + ", likes "
    val q7 = spark.sql(selectColumns + " from song_info_4 left join trackLikes on track_id=track")
    val q7Temp = q7.na.fill("0",Array("likes"))
    q7Temp.createOrReplaceTempView("song_info_5")
    spark.sqlContext.cacheTable("song_info_5")

    q7.printSchema()

    val top_genere_artists_df = genreQuery()
    top_genere_artists_df.createOrReplaceTempView("top_genere_artists")

    val queryy = selectColumns + ", top_50_count from song_info_5,top_genere_artists where artist_id = artistId3"

    val q9 = spark.sql(queryy)
    q9.createOrReplaceTempView("song_info_5")



    // join the features with download.csv
    val trainingDataDF = cleanTrainingData(spark,"/home/aditya/downloads_file/downloads.csv")
    val temp2 = song_info_columns + temp1 + ", likes "
    trainingDataDF.createOrReplaceTempView("tdata")
    trainingDataDF.printSchema()
    print(trainingDataDF.count())

    val featuresSelect = "select td_confidence, artist_hotttnesss,artist_familiarity,loudness,tempo,duration,song_hotttnesss,year"+ ", play_count" + ", song_count " +", weight_dot_freq "
    val featuresSelect2 = ",similar_artist_count, likes ,td_meanPrice,top_50_count,td_downloads "

    //val q8 = spark.sql(featuresSelect + featuresSelect2 + "from song_info_5,tdata where td_title = title and td_artist = artist_name")
    val q8 = spark.sql(featuresSelect + featuresSelect2 + ",td_artist from tdata inner join song_info_5 on td_title = title and td_artist = artist_name order by td_artist")
    println(q8.count())
//    val q8Temp = q8.na.fill("-60",Array("loudness"))
//                      .na.fill("-1",Array("tempo"))
//                      .na.fill("-1",Array("artist_hotttnesss"))
//                      .na.fill("-1",Array("artist_familiarity"))
//                      .na.fill("-1",Array("duration"))
//      .na.fill("-1",Array("song_hotttnesss"))
//      .na.fill("-1",Array("year"))
//      .na.fill("-1",Array("play_count"))
//      .na.fill(-1,Array("song_count"))
//      .na.fill(-1,Array("weight_dot_freq"))
//      .na.fill(-1,Array("similar_artist_count"))
//      .na.fill("-1",Array("likes"))
//      .na.fill(-1,Array("top_50_count"))



    q8.rdd.saveAsTextFile("features_small_temp")
    cleanFeatures(spark.sparkContext.textFile("features_small_temp"))



  }

  def getCSVIntoDF(spark:SparkSession,fileName:String,delimiter:String,header:String ="true"): DataFrame ={
    val df1 = spark.read
      .format("csv")
      .option("header", header) //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", delimiter)
      .load(fileName)

    df1
  }

  def cleanTrainingData(sparkSession: SparkSession,fileName:String):DataFrame={

    val training_data = getCSVIntoRDD(sparkSession.sparkContext,fileName,";")



    val temp2 = training_data.filter(row=> row.length == 5)
    println(temp2.count)
    val trainingDataRDD = temp2.map(row=> Row(cleanString(row(0)),cleanString(row(1)).toLowerCase,row(2),row(3),translateConfidence(row(4))))

    val customSchema = StructType(
      Array(
        StructField("td_artist",StringType,true),
        StructField("td_title",StringType,true),
        StructField("td_meanPrice",StringType,true),
        StructField("td_downloads",StringType,true),
        StructField("td_confidence",StringType,true))
    )

    sparkSession.sqlContext.createDataFrame(trainingDataRDD,customSchema)

  }

  def cleanString(s:String) :String ={

    s.replaceAll("[^A-Za-z0-9]","").toLowerCase()
  }

  def cleanFeatures(output:RDD[String]): Unit ={
    output.
      map(row => row.replace("[","").replace("]",""))
      .map(row=> row.split(","))
      .filter(row => row.length==17)
        .map(row=>calculatePopularity(row))
       //.filter(row => isAllDigits(row(11)) & isAllDigits(row(10)) & isDoubleNumber(row(9)) & isDoubleNumber(row(12)))
        .map(row=> convertArrayToSting(row))
        .coalesce(1)
      .saveAsTextFile("features_main")

  }

  def calculatePopularity(arr:Array[String]):Array[String] ={
    var x = arr(2).toDouble * arr(9).toInt * arr(11).toInt
    if(x<0){
      x = 0
    }
    var logx = Math.log10(x+1);
    var result:Array[String] = new Array[String](arr.length+1)
    for(i<-0 to arr.length-1){
      result(i) = arr(i)
    }
    result(arr.length) = logx.toString

    var temp = result(arr.length)
    result(arr.length) = result(arr.length-1)
    result(arr.length-1)= temp

    var temp2 = result(arr.length-1)
    result(arr.length-1) = result(arr.length-2)
    result(arr.length-2) = temp2

    result


  }



  def convertArrayToSting(arr:Array[String]):String ={
    var sb:StringBuilder = new StringBuilder()
    for(i<- 0 to arr.length-2) {
      if (arr.length == 18) {
          sb.append(arr(i))

        if (i < arr.length - 2) {
          sb.append(",")
        }
      }
    }
    sb.toString()
  }
  def isAllDigits(x: String) = x forall Character.isDigit

  def translateConfidence(c:String):String={
    if(c.equals("terrible")){
      return "0"
    }
    if(c.equals("poor")){
      return "1"
    }
    if(c.equals("average")){
      return "2"
    }
    if(c.equals("good")){
      return "3"
    }
    if(c.equals("very good")){
      return "4"
    }
    if(c.equals("excellent")){
      return "5"
    }
    else{
      return "-1"
    }
  }

  def getCSVIntoRDD(sc: SparkContext, fileName: String,sep:String): RDD[Array[String]] = {
    val fileRDD = sc.textFile(fileName)
    val header = fileRDD.first()
    fileRDD
      .filter(row => row != header)
      .map(row => row.split(sep))
  }

  def getArtistRelatedFeatures1(spark:SparkSession): DataFrame ={
    val customSchema = StructType(
      Array(
        StructField("artistId1",StringType,true),
        StructField("weight_dot_freq",DoubleType,true)
      ))


    val artist_terms = getCSVIntoRDD(spark.sparkContext,"/home/aditya/songs/all/artist_terms.csv",";")
    val artiIdPairRDD = artist_terms
      .filter(row => isDoubleNumber(row(2)) & isDoubleNumber(row(3)))
      .map(row => (row(0),(row(2).toDouble * row(3).toDouble)))
      .reduceByKey({case(a,b)=> a+b})
      .map({case(a,b) => Row(a,b)})

    spark.sqlContext.createDataFrame(artiIdPairRDD,customSchema)


  }

  def getAristRelatedFeatures2(spark:SparkSession):DataFrame={


    val similar_artists1 = getCSVIntoDF(spark,"/home/aditya/songs/all/similar_artists.csv",";")
    similar_artists1.createOrReplaceTempView("similar_artists")
    val q = spark.sql("select artist_id  as artistId2 ,count(similar_artist) as similar_artist_count from similar_artists group by artist_id")
    q
  }



  def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

  def cleanSongInfo(dataFrame: DataFrame,sparkSession: SparkSession) :DataFrame={
    //println(dataFrame.rdd.filter(row => row.getAs[String]("song_hotttnesss")=="NA").count())
    val cleanedRDD = dataFrame
      .rdd
      .map(row=> row.toString().replace("[","").replace("]",""))
      .filter(row=> row.split(",").length==12)
      .map(row => row.split(","))
      .map(row => changeNAValuesToZero(row))
      .map(row => Row(row(0),row(1),row(2),row(3),row(4),row(5),row(6),row(7),row(8),row(9),row(10),row(11)))

    val customSchema = StructType(
      Array(
        StructField("song_id",StringType,true),
        StructField("track_id",StringType,true),
        StructField("artist_id",StringType,true),
        StructField("artist_name",StringType,true),
        StructField("title",StringType,true),
        StructField("artist_hotttnesss",StringType,true),
        StructField("artist_familiarity",StringType,true),
        StructField("loudness",StringType,true),
        StructField("tempo",StringType,true),
        StructField("duration",StringType,true),
        StructField("song_hotttnesss",StringType,true),
        StructField("year",StringType,true)
      ))


  sparkSession.sqlContext.createDataFrame(cleanedRDD,customSchema)



  }
  def changeNAValuesToZero(arr: Array[String]):Array[String]={
    Array(arr(0),arr(1),arr(2),cleanString(arr(3)),cleanString(arr(4)),changeNaValueToZero(arr(5)),changeNaValueToZero(arr(6)),changeNaValueToZero(arr(7)),changeNaValueToZero(arr(8)),changeNaValueToZero(arr(9)),changeNaValueToZero(arr(10)),arr(11))
  }
  def changeNaValueToZero(s:String):String={
    if(!isDoubleNumber(s)){
      return "0"
    }
    else{
      return s
    }
  }
}
