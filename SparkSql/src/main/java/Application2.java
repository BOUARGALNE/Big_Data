import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.RelationalGroupedDataset;

import static org.apache.spark.sql.functions.*;
//meme exemple mais ave une autre methode

public class Application2 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tppark sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/exam");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> dfConsultation = ss.read().format("jdbc")
                .options(options)
                .option("dbtable", "CONSULTATION").load();
        Dataset<Row> dfMedecin=ss.read().format("jdbc")
                .options(options)
                .option("dbtable", "MEDECIN").load();
        Dataset<Row> dfPatient=ss.read().format("jdbc")
                .options(options)
                .option("dbtable", "PATIENT").load();
        Dataset<Row> dfNBConsultation=dfConsultation.select("DATE_CONSULTATION").groupBy("DATE_CONSULTATION").count();
        System.out.println("le nombre de consultation par jour :");
        dfNBConsultation.show();
        //####################### le nombre de consultation par medecin ###################################
        Dataset<Row> NBConsul_Medecin=dfConsultation.join(dfMedecin,dfConsultation.col("ID_MEDECIN")
                        .equalTo(dfMedecin.col("ID_MEDECIN")))
                        .select(dfMedecin.col("NOM"),dfMedecin.col("PRENOM"))
                        .groupBy(dfMedecin.col("NOM"),dfMedecin.col("PRENOM")).count();

        System.out.println("le nombre de consultation par medecin :");
        NBConsul_Medecin.show();
        //############################le nombre de Patient par medecin ##############################
        Dataset<Row> NBPatient_Medecin=dfConsultation.join(dfMedecin,dfConsultation.col("ID_MEDECIN")
                        .equalTo(dfMedecin.col("ID_MEDECIN")))
                .select(dfConsultation.col("ID_PATIENT"),dfMedecin.col("NOM"),dfMedecin.col("PRENOM")).distinct();
                NBPatient_Medecin=NBPatient_Medecin.groupBy(dfMedecin.col("NOM"),dfMedecin.col("PRENOM")).count();

        System.out.println("le nombre de patient par medecin :");
        NBPatient_Medecin.show();



    }
}
