import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class Aggregation {

    public static void main(String[] args) throws Exception {

        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Detecting Fraud Clicks")
                .master("local")
                .getOrCreate();

        // Aggregation
        Aggregation agg = new Aggregation();

        Dataset<Row> dataset = agg.loadCSVDataSet("/home/chris/.kaggle/competitions/talkingdata-adtracking-fraud-detection/mnt/ssd/kaggle-talkingdata2/competition_files/train_sample.csv", spark);
        dataset = agg.changeTimestempToLong(dataset);
        dataset = agg.averageValidClickCount(dataset);
        dataset = agg.clickTimeDelta(dataset);
        dataset = agg.countClickInTenMinutes(dataset);

        long start = System.currentTimeMillis();

        List<String> logs_with_features = dataset.map(row->row.toString(), Encoders.STRING()).collectAsList();
        String[][] contents = new String[(int)dataset.count()][11];
        for (int i =0; i<logs_with_features.size();i++){
            String str_to_split = logs_with_features.get(i);
            String[] tmp = str_to_split.substring(1,str_to_split.length()-1).split(",");
            contents[i] = tmp;
        }

        long end = System.currentTimeMillis();
        System.out.println("JK's Procedure time elapsed : " + (end-start)/1000.0);

        start = System.currentTimeMillis();
        List<String> stringDataset = dataset.toJSON().collectAsList();
        end = System.currentTimeMillis();
        System.out.println("Steve's Procedure 1 time elapsed : " + (end-start)/1000.0);
        new GUI(stringDataset, contents);


    }


    private Dataset<Row> loadCSVDataSet(String path, SparkSession spark){
        // Read SCV to DataSet
        return spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);
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
                (count("utc_click_time").over(w)).minus(1));    //TODO 본인것 포함할 것인지 정해야함.
        return newDF;
    }
}