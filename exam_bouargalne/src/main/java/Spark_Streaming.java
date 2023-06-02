import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;


public class Spark_Streaming {
    public static void main(String[] args) throws  InterruptedException {
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("StreamProcessing").setMaster("local[*]");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(5000));
        JavaDStream<String> dStreamAvion = streamingContext.textFileStream("hdfs://localhost:8970/input");

        JavaDStream<List<String>> avions = dStreamAvion.map(line -> Arrays.asList(line.split("\n")));
        JavaDStream<List<String>> avionID = avions.map(line -> Arrays.asList(line.get(0).split(",")));
        JavaPairDStream<String, Integer> incidentsCount = avionID
                .mapToPair(avion -> new Tuple2<>(avion.get(2), 1))
                .reduceByKey((count1, count2) -> count1 + count2);

        JavaDStream<Tuple2<String, Integer>> avionMaxIncid = incidentsCount
                .reduce((avion1, avion2) -> avion1._2() > avion2._2() ? avion1 : avion2);

        avionMaxIncid.print();

        streamingContext.start();

        streamingContext.awaitTermination();
    }

}
