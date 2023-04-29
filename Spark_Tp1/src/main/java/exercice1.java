import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class exercice1 {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("etudiant").setMaster("local[*]");
        JavaSparkContext javaSparkContext= new JavaSparkContext(sparkConf);
        JavaRDD<Double> rdd1= javaSparkContext.parallelize(Arrays.asList(12.3,10.4,2.5,9.5,7.1,15.0,11.6));
        JavaRDD<Double> rdd2=rdd1.flatMap(s -> Arrays.asList(Math.exp(s)).iterator());
        JavaRDD<Double> rdd3=rdd2.filter((a)->a>10);
        JavaRDD<Double> rdd4=rdd2.filter((a)->a<20);
        JavaRDD<Double> rdd5=rdd2.filter((a)->a<20*Math.sin(a));
        JavaRDD<Double> rdd6=rdd3.union(rdd4);
        JavaRDD<Double> rdd71=rdd5.map((a)->a+7);
        JavaRDD<Double> rdd81=rdd6.map((a)->a+Math.sin(a));
        JavaPairRDD<Double, Integer> rdd8 = rdd81.mapToPair(a -> new Tuple2<>(a, 1));   //.mapToPair((a -> Tuple2.apply(a)));
        JavaPairRDD<Double, Integer> rdd7 = rdd71.mapToPair(a -> new Tuple2<>(a, 1));   //.mapToPair((a -> Tuple2.apply(a)));
        JavaPairRDD<Double, Integer> rdd82 = rdd8.reduceByKey((freq1, freq2) -> freq1 + freq2);
        JavaPairRDD<Double, Integer> rdd72 = rdd7.reduceByKey((freq1, freq2) -> freq1 + freq2);
        JavaRDD<Double> rdd9= rdd72.union(rdd82).keys();
        JavaRDD<Double> rdd10 = rdd9.sortBy(word -> word.doubleValue(), false,4);
        rdd10.foreach(wordCount -> System.out.println(wordCount.doubleValue() ));
    }
    }
