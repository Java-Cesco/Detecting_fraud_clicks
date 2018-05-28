import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.*;

public class calForwardTimeDelta {
 static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Cesco");
 static JavaSparkContext sc = new JavaSparkContext(conf);

 public static void main(String[] args) throws Exception{
     //Create Session
     SparkSession spark = SparkSession
             .builder()
             .appName("Detecting Fraud Clicks")
             .getOrCreate();

     //run methods hereu
     calcDelta(spark);
 }

 private static void calcDelta(SparkSession spark){
     // put the path the file you gonna deal with being placed
     String filepath =  "train_sample.csv";

     // create Dataset from files
     Dataset<Row> logDF = spark.read()
             .format("csv")
             .option("inferSchema", "true")
             .option("header","true")
             .load(filepath);

     // cast timestamp(click_time, attributed_time) type to long type

     //add column for long(click_time)
     Dataset<Row> newDF = logDF.withColumn("utc_click_time", logDF.col("click_time").cast("long"));
     //add column for long(attributed_time)
     newDF = newDF.withColumn("utc_attributed_time", logDF.col("attributed_time").cast("long"));
     //drop timestamp type columns
     newDF = newDF.drop("click_time").drop("attributed_time");
     newDF.createOrReplaceTempView("logs");

     WindowSpec w = Window.partitionBy ("ip")
             .orderBy("utc_click_time");

     newDF = newDF.withColumn("lag(utc_click_time)", lag("utc_click_time",1).over(w));
     newDF.where("ip=10").show();
     newDF = newDF.withColumn("delta", when(col("lag(utc_click_time)").isNull(),lit(0)).otherwise(col("utc_click_time")).minus(when(col("lag(utc_click_time)").isNull(),lit(0)).otherwise(col("lag(utc_click_time)"))));
     //newDF = newDF.withColumn("delta", datediff());
     newDF = newDF.drop("lag(utc_click_time)");
     newDF = newDF.orderBy("ip");

     newDF.show();
 }

}
