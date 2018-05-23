import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;


public class AvgAdvTime {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("train_sample.csv");
        
        df.show();
        df.createOrReplaceTempView("logs");
        
        Dataset<Row> ds = spark.sql("SELECT ip, app, click_time, is_attributed" +
                "FROM logs " +
                "ORDER BY click_time");
        ds.show();

        System.out.println();
    }
}