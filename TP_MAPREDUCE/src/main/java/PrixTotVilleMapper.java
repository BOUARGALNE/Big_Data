import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PrixTotVilleMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String villes[]= value.toString().split(" ");
        String ville=villes[1];
        String date =villes[0];
        String anne[]=date.split("/");
        int prix=Integer.parseInt(villes[3]);
        if (anne[2].equals((String) context.getConfiguration().get("annee"))){
            context.write(new Text(ville),new IntWritable(prix));
        }

    }
}
