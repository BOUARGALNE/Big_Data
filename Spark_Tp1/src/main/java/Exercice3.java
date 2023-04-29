import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class Exercice3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
        conf.setAppName("vente");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("2020.csv");
        JavaPairRDD<String,Double> temp1=lines.mapToPair((word)->new Tuple2<>(word.split(",")[2],Double.
                parseDouble(word.split(",")[3])));
        JavaPairRDD<String,Double> temp=temp1.reduceByKey((a,b)->a+b);
        List<Tuple2<String,Double>> tempInfo=null;
        tempInfo= temp.collect();
        for (Tuple2<String,Double> vente:tempInfo) {
            System.out.println(vente._1()+" "+vente._2());
        }
    }
}
