import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.sql.SparkSession;

public class LoadModel {
    public static void main(String args[]) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Detecting Fraud Clicks")
                .master("local")
                .getOrCreate();

        PipelineModel model = PipelineModel.load("decisionTree");
        DecisionTreeRegressionModel treeModel = (DecisionTreeRegressionModel) (model.stages()[1]);
        System.out.println(treeModel.toDebugString());
    }
}
