package detact;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Utill {
    
    public static Dataset<Row> loadCSVDataSet(String path, SparkSession spark){
        // Read SCV to DataSet
        return spark.read().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);
    }

    public static void saveCSVDataSet(Dataset<Row> dataset, String path){
        // Read SCV to DataSet
        dataset.write().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .save(path);
    }
}
