package detact.ML;

import detact.Aggregation;
import detact.Utill;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


// DecisionTree Model

public class DecisionTree {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 1) {
            System.out.println("Usage: java -jar decisionTree.jar <agg_path>");
            System.exit(0);
        }
        
        String agg_path = args[0];
        
        //Create Session
        SparkSession spark = SparkSession
                .builder()
                .appName("Detecting Fraud Clicks")
                .master("local")
                .getOrCreate();
        
        // load aggregated dataset
        Dataset<Row> resultds = Utill.loadCSVDataSet(agg_path, spark);

        // show Dataset schema
//        System.out.println("schema start");
//        resultds.printSchema();
//        String[] cols = resultds.columns();
//        for (String col : cols) {
//            System.out.println(col);
//        }
//        System.out.println("schema end");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{
                        "ip", 
                        "app", 
                        "device", 
                        "os", 
                        "channel", 
                        "utc_click_time", 
                        "avg_valid_click_count", 
                        "click_time_delta",
                        "count_click_in_ten_mins"
                })
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(resultds);
        
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(2)
                .fit(output);

        // Split the result into training and test sets (30% held out for testing).
        Dataset<Row>[] splits = output.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Train a detact.DecisionTreeionTree model.
        DecisionTreeRegressor dt = new DecisionTreeRegressor()
                .setFeaturesCol("indexedFeatures")
                .setLabelCol("is_attributed")
                .setMaxDepth(10);

        // Chain indexer and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{featureIndexer, dt});

        // Train model. This also runs the indexer.
        PipelineModel model = pipeline.fit(trainingData);

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);

        // Select example rows to display.
        predictions.select("is_attributed", "features").show(5);

        // Select (prediction, true label) and compute test error.
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("is_attributed")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        double rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test result = " + rmse);
        
        DecisionTreeRegressionModel treeModel =
                (DecisionTreeRegressionModel) (model.stages()[1]);
        System.out.println("Learned regression tree model:\n" + treeModel.toDebugString());
        
    }
    
}
