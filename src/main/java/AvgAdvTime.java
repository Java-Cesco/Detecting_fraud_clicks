import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;


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
        
        // cast timestamp to long
        Dataset<Row> newdf = df.withColumn("utc_click_time", df.col("click_time").cast("long"));
        newdf = newdf.withColumn("utc_attributed_time", df.col("attributed_time").cast("long"));
        newdf = newdf.drop("click_time").drop("attributed_time");

        WindowSpec w = Window.partitionBy("ip", "app")
                .orderBy("utc_click_time")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());
        
        newdf = newdf.withColumn("cum_count_click", count("utc_click_time").over(w));
        newdf = newdf.withColumn("cum_sum_attributed", sum("is_attributed").over(w));        
        newdf = newdf.withColumn("avg_efficient", col("cum_sum_attributed").divide(col("cum_count_click")));
        newdf.where("ip == '5348' and app == '19'").show();
        newdf.printSchema();
        
    }
}