import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
public class Exercice2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf();
        conf.setAppName("vente").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("ventes.txt");
        JavaRDD<String> dates=lines.map((word)->word.split(" ")[0].split("/")[2]);
        List<String> Dates= dates.collect();
        List<String> annees=new ArrayList<>();
        for (String d:Dates) {
            if (!annees.contains((String) d)){
               annees.add(d);
            }
        }
         List<JavaPairRDD<String,Double>> Rdd=new ArrayList<>();
        JavaPairRDD<String,Double> ville_vente=null;
        JavaPairRDD<String,Double> Ville_Tot_ventes=null;
        List<Tuple2<String,Double>> wordCount=null;
        for (String d:annees) {
            ville_vente=lines.mapToPair((word)->new Tuple2<>(word.split(" ")[0].split("/")[2]+" "+word.split(" ")[1],
                            Double.parseDouble(word.split(" ")[3])))
                    .filter(a->a._1.contains(d));
            Ville_Tot_ventes=ville_vente.reduceByKey((a,b)->a+b);
            Rdd.add(Ville_Tot_ventes);
        }
        for (JavaPairRDD<String,Double> t:Rdd) {
            wordCount=t.collect();
            for (Tuple2<String,Double> vente:wordCount) {
                System.out.println(vente._1()+" "+vente._2());
            }
        }

    }
}
