import com.oracle.jrockit.jfr.DataType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;


import java.util.Calendar;

import java.sql.Time;
import java.sql.Timestamp;

import java.util.Arrays;
import java.util.List;

class data {
    private int ip;
    private int app;
    private int device;
    private int os;
    private int channel;
    //private int date click_time;
    //private int date a
}

public class MapExample {

    static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Cesco");
    static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        /*StructType schema = new StructType()
                .add("ip","int")
                .add("app","int")
                .add("device","int")
                .add("os","int")
                .add("channel","int")
                .add("click_time","datetime2")
                .add("attributed_time","datetime2")
                .add("is_attributed","int");*/
        Dataset<Row> df2 = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema","true")
                .option("header", "true")
                .load("train_sample.csv");

        Dataset<Row> df=df2.select("ip","click_time");
        df.createOrReplaceTempView("data");

        Dataset<Row> mindf = spark.sql("select ip, min(click_time) as first_click_time from data group by ip order by ip");
        mindf.createOrReplaceTempView("mindf");

        Dataset<Row> df3 = spark.sql("select * from data natural join mindf order by ip,click_time");
        df3.createOrReplaceTempView("df3");

        //df3.na().fill("2020-01-01 00:00");
        Dataset<Row> newdf = df3.withColumn("utc_click_time",df3.col("click_time").cast("long"));
        newdf = newdf.withColumn("utc_fclick_time",df3.col("first_click_time").cast("long"));
        newdf=newdf.drop("click_time"); newdf=newdf.drop("first_click_time");

        newdf = newdf.withColumn("within_ten",((newdf.col("utc_click_time").minus(newdf.col("utc_fclick_time"))).divide(60)));

        newdf = newdf.withColumn("check_ten",newdf.col("within_ten").gt((long)600));
        Dataset<Row> newdf2=newdf.select("ip","check_ten");

        //newdf = newdf.withColumn("check_ten",newdf.col("within_ten").notEqual((long)0));
        //newdf = newdf.withColumn("check_ten",(newdf.col("within_ten") -> newdf.col("within_ten") < 11 ? 1 : 0));

        newdf2.show();

        /*Dataset<Row> df4= spark.sql("select ip, cast((convert(bigint,click_time)) as decimal)  from df3");
        spark.udf().register("timestamp_diff",new UDF1<DataTypes.LongType,DataTypes.LongType>(){
            public long call(long arg1,long arg2) throws Exception{
                long arg3 = arg1-arg2;
                return arg3;
            }
        }, DataTypes.LongType);

        df3.withColumn("term",df3.col("click_time").cast("timestamp")-df3.col("first_click_time").cast("timestamp"));
        Dataset<Row> df4=df3.toDF();
        df4 = df4.withColumn("Check10Min", df3.select(df4("click_time").minus(df4("first_click_time")));*/

    }
}