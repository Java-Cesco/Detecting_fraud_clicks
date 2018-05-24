import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.Aggregator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.spark.sql.functions.unix_timestamp;


public class AvgAdvTime {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();
        
        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("train_sample.csv");
        df.printSchema();
        
        // cast timestamp to long
        Dataset<Row> newdf = df.withColumn("utc_click_time", df.col("click_time").cast("long"));
        newdf = newdf.withColumn("utc_attributed_time", df.col("attributed_time").cast("long"));
        newdf.show();
    }
}