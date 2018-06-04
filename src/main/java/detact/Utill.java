package detact;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Utill {
    
    public static Dataset<Row> loadCSVDataSet(String path, SparkSession spark){
        // Read SCV to DataSet
        return spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);
    }

    public static void saveCSVDataSet(Dataset<Row> dataset, String path){
        // Read SCV to DataSet
        dataset.repartition(1)
                .write().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .save(path);
    }
}
