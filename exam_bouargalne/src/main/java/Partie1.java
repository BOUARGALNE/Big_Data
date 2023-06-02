
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Partie1 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("exam sql").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/DB_AEROPORT");
        options.put("user","root");
        options.put("password","");

        Dataset<Row> dfEmp = ss.read().format("jdbc")
                .options(options)
                .option("query","select ID_VOL, V.DATE_DEPART , COUNT(ID_PASSAGER) as NOMBRE from RESERVATIONS , VOLS as V " +
                        "WHERE RESERVATIONS.ID_VOL = V.ID " +
                        "GROUP BY ID_VOL")
                .load();
        dfEmp.show();
        Date now = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String currentDate = formatter.format(now);
        Dataset<Row> dfEmp2 = ss.read().format("jdbc")
                .options(options)
                .option("query", "SELECT ID, DATE_DEPART, DATE_ARRIVE FROM VOLS AS V " +
                        "WHERE DATE(V.DATE_DEPART) <= '" + currentDate + "' " +
                        "AND DATE(V.DATE_ARRIVE) >= '" + currentDate + "'")
                .load();
        dfEmp2.show();

    }
}
