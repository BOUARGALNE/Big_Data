import org.apache.spark.sql.SparkSession;

public class salaries {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().master("local[*]").getOrCreate();

    }
}

