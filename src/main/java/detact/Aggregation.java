package detact;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class Aggregation {

    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println("Usage: java -jar aggregation.jar <data_path> <result_path>");
            System.exit(0);
        }
        
        String data_path = args[0];
        String result_path = args[1];
        
        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Detecting Fraud Clicks")
                .master("local")
                .config("spark.driver.memory", "2g")
                .getOrCreate();
        
        // detact.Aggregation
        Aggregation agg = new Aggregation();
        
        Dataset<Row> dataset = Utill.loadCSVDataSet(data_path, spark);
        dataset = agg.changeTimestempToLong(dataset);
        dataset = agg.averageValidClickCount(dataset);
        dataset = agg.clickTimeDelta(dataset);
        dataset = agg.countClickInTenMinutes(dataset);
        
        // test
        dataset.where("ip == '5348' and app == '19'").show(10);
        
        // Save to scv
        Utill.saveCSVDataSet(dataset, result_path);
    }
    
    private Dataset<Row> changeTimestempToLong(Dataset<Row> dataset){
        // cast timestamp to long
        Dataset<Row> newDF = dataset.withColumn("utc_click_time", dataset.col("click_time").cast("long"));
        newDF = newDF.withColumn("utc_attributed_time", dataset.col("attributed_time").cast("long"));
        newDF = newDF.drop("click_time").drop("attributed_time");
        return newDF;
    }
         
    private Dataset<Row> averageValidClickCount(Dataset<Row> dataset){
        // set Window partition by 'ip' and 'app' order by 'utc_click_time' select rows between 1st row to current row
        WindowSpec w = Window.partitionBy("ip", "app")
                .orderBy("utc_click_time")
                .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        // aggregation
        Dataset<Row> newDF = dataset.withColumn("cum_count_click", count("utc_click_time").over(w));
        newDF = newDF.withColumn("cum_sum_attributed", sum("is_attributed").over(w));
        newDF = newDF.withColumn("avg_valid_click_count", col("cum_sum_attributed").divide(col("cum_count_click")));
        newDF = newDF.drop("cum_count_click", "cum_sum_attributed");
        return newDF;
    }

    private Dataset<Row> clickTimeDelta(Dataset<Row> dataset){
        WindowSpec w = Window.partitionBy ("ip")
                .orderBy("utc_click_time");

        Dataset<Row> newDF = dataset.withColumn("lag(utc_click_time)", lag("utc_click_time",1).over(w));
        newDF = newDF.withColumn("click_time_delta", when(col("lag(utc_click_time)").isNull(),
                lit(0)).otherwise(col("utc_click_time")).minus(when(col("lag(utc_click_time)").isNull(),
                lit(0)).otherwise(col("lag(utc_click_time)"))));
        newDF = newDF.drop("lag(utc_click_time)");
        return newDF;
    }
    
    private Dataset<Row> countClickInTenMinutes(Dataset<Row> dataset){
        WindowSpec w = Window.partitionBy("ip")
                .orderBy("utc_click_time")
                .rangeBetween(Window.currentRow(),Window.currentRow()+600);

        Dataset<Row> newDF = dataset.withColumn("count_click_in_ten_mins",
                (count("utc_click_time").over(w)).minus(1));  
        return newDF;
    }
    
}
