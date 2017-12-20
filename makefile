SPARK_HOME = /home/aditya/spark-2.2.0-bin-hadoop2.7
CLASSPATH=`echo $(SPARK_HOME)/jars/*.jar | tr ' ' :`
SONG_INFO = /home/aditya/songs/all/song_info.csv
ARTIST_TERMS = /home/aditya/songs/all/artist_terms.csv
SIMILAR_ARTISTS = /home/aditya/songs/all/similar_artists.csv
OUTPUT_DIR_WITH_CACHE = output2
OUTPUT_DIR_WITHOUT_CACHE = output1
ITERATIONS = 10
all: build

build:
	scalac -cp $(CLASSPATH) src/RandomForestImpl.scala -d RandomForestImpl.jar

run:
	$(SPARK_HOME)/bin/spark-submit --class RandomForestImpl \
        --master local[*] --deploy-mode client --executor-memory 4g \
        --name RandomForestImpl --conf "spark.app.id=RandomForestImpl" \
        RandomForestImpl.jar

