import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

//ip,app,device,os,channel,click_time,attributed_time,is_attributed
//87540,12,1,13,497,2017-11-07 09:30:38,,0
class Record implements Serializable {
    int ip;
    int app;
    int device;
    int os;
    int channel;
    Calendar clickTime;
    Calendar attributedTime;
    boolean isAttributed;

    // constructor , getters and setters
    public Record(int pIp, int pApp, int pDevice, int pOs, int pChannel, Calendar pClickTime, Calendar pAttributedTime, boolean pIsAttributed) {
        ip = pIp;
        app = pApp;
        device = pDevice;
        os = pOs;
        channel = pChannel;
        clickTime = pClickTime;
        attributedTime = pAttributedTime;
        isAttributed = pIsAttributed;
    }
}

public class MapExample {

    static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Cesco");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    static SQLContext sqlContext = new SQLContext(sc);
    
    public static void main(String[] args) throws Exception {
        JavaRDD<String> file = sc.textFile("/Users/hyeongyunmun/Dropbox/DetectFraudClick/data/train.csv");

        final String header = file.first();
        JavaRDD<String> data = file.filter(line -> !line.equalsIgnoreCase(header));

        JavaRDD<Record> records = data.map((line) -> {
            String[] fields = line.split(",");
            Record sd = new Record(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3]), Integer.parseInt(fields[4]), DateUtil.CalendarFromString(fields[5]), DateUtil.CalendarFromString(fields[6]), "1".equalsIgnoreCase(fields[7].trim()));
            return sd;
        });
    }
}
