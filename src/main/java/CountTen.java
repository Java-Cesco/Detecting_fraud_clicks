import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;


public class CountTen {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("./data/train.csv");

        // cast timestamp to long
        Dataset<Row> newdf = df.withColumn("utc_click_time", df.col("click_time").cast("long"));
        newdf = newdf.withColumn("utc_attributed_time", df.col("attributed_time").cast("long"));
        newdf = newdf.drop("click_time").drop("attributed_time");

        WindowSpec w = Window.partitionBy("ip")
                .orderBy("utc_click_time");
//                .rowsBetween(Window.currentRow(), Window.unboundedPreceding());   //Boundary end is not a valid integer: -9223372036854775808

        newdf = newdf.withColumn("is_clicked_in_ten_mins",
                (lead(col("utc_click_time"),1).over(w).minus(col("utc_click_time")).lt((long)600)).cast("long"));

        newdf.where("ip == '117898'").show(false);
    }
}