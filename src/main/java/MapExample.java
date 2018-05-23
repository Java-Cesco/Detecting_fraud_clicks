import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
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

import java.util.*;

//ip,app,device,os,channel,click_time,attributed_time,is_attributed
//87540,12,1,13,497,2017-11-07 09:30:38,,0
class Record implements Serializable {
    Integer ip;
    Integer app;
    Integer device;
    Integer os;
    Integer channel;
    Calendar clickTime;
    Calendar attributedTime;
    Boolean isAttributed;
    Integer clickInTenMins;

    // constructor , getters and setters
    public Record(int pIp, int pApp, int pDevice, int pOs, int pChannel, Calendar pClickTime, Calendar pAttributedTime, boolean pIsAttributed) {
        ip = new Integer(pIp);
        app = new Integer(pApp);
        device = new Integer(pDevice);
        os = new Integer(pOs);
        channel = new Integer(pChannel);
        clickTime = pClickTime;
        attributedTime = pAttributedTime;
        isAttributed = new Boolean(pIsAttributed);
        clickInTenMins = new Integer(0);
    }

    public Record(int pIp, int pApp, int pDevice, int pOs, int pChannel, Calendar pClickTime, Calendar pAttributedTime, boolean pIsAttributed, int pClickInTenMins) {
        ip = new Integer(pIp);
        app = new Integer(pApp);
        device = new Integer(pDevice);
        os = new Integer(pOs);
        channel = new Integer(pChannel);
        clickTime = pClickTime;
        attributedTime = pAttributedTime;
        isAttributed = new Boolean(pIsAttributed);
        clickInTenMins = new Integer(pClickInTenMins);
    }
}

class RecordComparator implements Comparator<Record> {
    @Override
    public int compare(Record v1 , Record v2) {
//        if(a.ano < b.ano) return -1;
//        else if(a.ano == b.ano) return 0;
//        else return 1;
        if (v1.ip.compareTo(v2.ip) == 0) {
            return v1.clickTime.compareTo(v2.clickTime);
        }
        return  v1.ip.compareTo(v2.ip);
    }
}

public class MapExample {

    static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Cesco");
    static JavaSparkContext sc = new JavaSparkContext(conf);
    static SQLContext sqlContext = new SQLContext(sc);
    
    public static void main(String[] args) throws Exception {
        JavaRDD<String> file = sc.textFile("/Users/hyeongyunmun/Dropbox/DetectFraudClick/data/train.csv", 1);

        final String header = file.first();
        JavaRDD<String> data = file.filter(line -> !line.equalsIgnoreCase(header));

        JavaRDD<Record> records = data.map(line -> {
            String[] fields = line.split(",");
            Record sd = new Record(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3]), Integer.parseInt(fields[4]), DateUtil.CalendarFromString(fields[5]), DateUtil.CalendarFromString(fields[6]), "1".equalsIgnoreCase(fields[7].trim()));
            return sd;
        });

//        JavaRDD<Tuple4<Integer,Double,Long,Integer>> secondSortRDD = firstSortRDD.keyBy(new Function<Tuple4<Integer, Double, Long, Integer>, Tuple2<Double, Long>>(){
//            @Override
//            public Tuple2<Double, Long> call(Tuple4<Integer, Double, Long, Integer> value) throws Exception {
//                return new Tuple2(value._2(),value._3());
//            }}).sortByKey(new TupleComparator()).values();

        JavaRDD<Record> firstSorted = records.sortBy(new Function<Record, Calendar>() {
            @Override
            public Calendar call(Record record) throws Exception {
                return record.clickTime;
            }
        }, true, 1);

        JavaRDD<Record> sortedRecords = firstSorted.sortBy(new Function<Record, Integer>() {
            @Override
            public Integer call(Record record) throws Exception {
                return record.ip.intValue();
            }
        }, true, 1);


        /*
        //두개를 한번에 정렬해보려 했지만 실패
        JavaRDD<Record> sortedRecords = records.keyBy(new Function<Record, Record>(){
            @Override
            public Record call(Record record) throws Exception {
                return new Record(record.ip, record.app, record.device, record.os, record.channel, record.clickTime, record.attributedTime, record.isAttributed);
            }}).sortByKey(new RecordComparator()).values();
        */

//        System.out.println("sortedRecords");
//        sortedRecords.foreach(record -> {System.out.println(record.ip + " " + record.clickTime.getTime());});

//        System.out.println("make result");
        /*
        //map의 다음것을 가져오려했지만 실패
        JavaRDD<Record> result = sortedRecords.map(record -> {
            System.out.println("make addTen");
            Calendar addTen = Calendar.getInstance();
            addTen.setTime(record.clickTime.getTime());
            addTen.add(Calendar.MINUTE, 10);

            System.out.println("make count");
            int count = 0;
            for (Record temp: sortedRecords.collect()) {
                if (temp.ip.compareTo(record.ip) == 0 && temp.clickTime.compareTo(record.clickTime) > 0 && temp.clickTime.compareTo(addTen)< 0)
                    count++;
            }

            return new Record(record.ip, record.app, record.device, record.os, record.channel, record.clickTime, record.attributedTime, record.isAttributed, count);
        });
        */
//        System.out.println("result");
//        result.foreach(record -> {System.out.println(record.ip + " " + record.clickTime.getTime());});

        /*

        for (final ListIterator<String> it = list.listIterator(); it.hasNext();) {
            final String s = it.next();
            System.out.println(it.previousIndex() + ": " + s);
        }

        for (ListIterator<Record> it = sortedRecords.collect().listIterator(); it.hasNext(); it = it.nextIndex()) {
            it.
            if (temp.ip.compareTo(record.ip) == 0 && temp.clickTime.compareTo(record.clickTime) > 0 && temp.clickTime.compareTo(addTen)< 0)
                count++;
        }
        */


        List<Record> list = sortedRecords.collect();

        List<Record> resultList = new ArrayList<Record>();
        for (int i = 0; i < list.size(); i++) {
            //System.out.println(list.get(i).ip);

            Record record = list.get(i);

            Calendar addTen = Calendar.getInstance();
            addTen.setTime(record.clickTime.getTime());
            addTen.add(Calendar.MINUTE, 10);

            int count = 0;

            for (int j = i+1; j < list.size() && list.get(j).ip.compareTo(record.ip) == 0
                    && list.get(j).clickTime.compareTo(record.clickTime) > 0 &&list.get(j).clickTime.compareTo(addTen) < 0; j++)
                count++;

            resultList.add(new Record(record.ip, record.app, record.device, record.os, record.channel, record.clickTime, record.attributedTime, record.isAttributed, count));

        }


        JavaRDD<Record> result = sc.parallelize(resultList);
        result.foreach(record -> {System.out.println(record.ip + " " + record.clickTime.getTime() + " " + record.clickInTenMins);});

    }
}
