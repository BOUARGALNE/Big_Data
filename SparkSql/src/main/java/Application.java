import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Application {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("tppark sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/exam");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> dfEmp = ss.read().format("jdbc")
                .options(options)
                .option("query","select DATE_CONSULTATION, COUNT(ID_CONSULTATION) from CONSULTATION GROUP BY DATE_CONSULTATION")
                .load();
        dfEmp.show();
        Dataset<Row> dfEmp2 = ss.read().format("jdbc")
                .options(options)
                .option("query","select MEDECIN.NOM, MEDECIN.PRENOM, COUNT(CONSULTATION.ID_CONSULTATION) as NbRConsultation " +
                        "from CONSULTATION, MEDECIN WHERE MEDECIN.ID_MEDECIN=CONSULTATION.ID_MEDECIN " +
                        "GROUP BY CONSULTATION.ID_MEDECIN")
                .load();
        dfEmp2.show();
       /* Dataset<Row> dfEmp3 = ss.read().format("jdbc")
                .options(options)
                .option("query","select MEDECIN.NOM, COUNT(CONSULTATION.ID_PATIENT) as NbRPatient from CONSULTATION, MEDECIN,PATIENT WHERE MEDECIN.ID_MEDECIN=CONSULTATION.ID_MEDECIN " +
                        "GROUP BY CONSULTATION.ID_MEDECIN")
                .load();
        dfEmp3.show();*/

        Dataset<Row> dfEmp4 = ss.read().format("jdbc")
                .options(options)
                .option("query","select MEDECIN.NOM, COUNT( DISTINCT CONSULTATION.ID_PATIENT) " +
                        "from CONSULTATION, MEDECIN WHERE MEDECIN.ID_MEDECIN=CONSULTATION.ID_MEDECIN "+
                        "GROUP BY CONSULTATION.ID_MEDECIN")
                .load();
        dfEmp4.show();
    }
}