import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;


import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.filechooser.FileFilter;
import javax.swing.table.DefaultTableModel;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

public class GUI extends JFrame {
    JTabbedPane tab = new JTabbedPane();

    public GUI() {
        super("CESCO");
        tab.addTab("main", new CreateTable_tab());
        tab.addTab("graphics", new PngPane());
        add(tab);

        setSize(1280, 1024); // 윈도우의 크기 가로x세로
        setVisible(true); // 창을 보여줄떄 true, 숨길때 false
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // x 버튼을 눌렀을때 종료
    }

   public static void main(String args[]) {

       new GUI();
   }
}

class PngPane extends JPanel {
    public PngPane() {
        super();
        ImageIcon image = new ImageIcon("./visualize.png");
        JScrollPane pan = new JScrollPane();
        JLabel label = new JLabel("", image, JLabel.CENTER);
        pan.setViewportView(label);
        setLayout(new BorderLayout());
        add(pan, BorderLayout.CENTER);
        JLabel acc = new JLabel("accuracy: 98.12");
        add(acc,BorderLayout.NORTH);
    }
}
class SharedArea{
    Dataset<Row> data;
}
class CreateTable_tab extends JPanel{
    public JPanel centre_pane = new JPanel();
    public JPanel south_pane = new JPanel();

    public JScrollPane pan1 = new JScrollPane();
    public JTable table1 = new JTable();
    public JButton btn1 = new JButton("CONFIRM");

    public JScrollPane pan2 = new JScrollPane();
    public JTable table2 = new JTable();


    public JScrollPane pan3 = new JScrollPane();
    public JTable table3 = new JTable();

    private DefaultTableModel tableModel1 = new DefaultTableModel(new Object[]{"unknown"},1);
    private DefaultTableModel tableModel2 = new DefaultTableModel(new Object[]{"unknown"},1);
    private DefaultTableModel tableModel3 = new DefaultTableModel(new Object[]{"unknown"},1);

    public CsvFile_chooser temp = new CsvFile_chooser();
    private String current_state="100";

    public CreateTable_tab(){
        super();
        setLayout(new BorderLayout());

        //csvFile_chooser
        add(temp, BorderLayout.NORTH);

        // sub Panel 1
        centre_pane.setLayout(new GridLayout(1, 3));
        pan1.setViewportView(table1);
        centre_pane.add(pan1);

        // sub Panel 2
        pan2.setViewportView(table2);
        centre_pane.add(pan2);

        // sub Panel 3
        pan3.setViewportView(table3);
        centre_pane.add(pan3);
        add(centre_pane, BorderLayout.CENTER);
        //sub Panel 4
        south_pane.setLayout(new FlowLayout());
        south_pane.add(btn1);

        btn1.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {


                if(temp.is_selected) {
                    String path = temp.selected_file.getAbsolutePath();
                    // 1st Column Raw Data
                    SparkSession spark = SparkSession
                            .builder()
                            .appName("Detecting Fraud Clicks")
                            .master("local")
                            .getOrCreate();

                    // Aggregation
                    Aggregation agg = new Aggregation();

                    // Raw data
                    TableCreator table_maker = new TableCreator();

                    Dataset<Row> dataset = agg.loadCSVDataSet(path, spark);
                    if(current_state.equals("100")){
                        List<String> stringDataset_Raw = dataset.toJSON().collectAsList();
                        String[] header_r = {"ip", "app", "device", "os", "channel", "click_time", "is_attributed"};
                        table1.setModel(table_maker.getTableModel(stringDataset_Raw, header_r));
                        current_state="200";
                    }else if(current_state.equals("200")){
                        // 2nd Column Data with features
                        // Adding features
                        dataset = agg.changeTimestempToLong(dataset);
                        dataset = agg.averageValidClickCount(dataset);
                        dataset = agg.clickTimeDelta(dataset);
                        dataset = agg.countClickInTenMinutes(dataset);
                        List<String> stringDataset_feat = dataset.toJSON().collectAsList();
                        String[] header_f = {"ip", "app", "device", "os", "channel", "is_attributed", "click_time",
                                "avg_valid_click_count", "click_time_delta", "count_click_in_ten_mins"};
                        table2.setModel(table_maker.getTableModel(stringDataset_feat, header_f));
                        current_state="300";
                    }else if(current_state.equals("300")){
                        dataset = agg.changeTimestempToLong(dataset);
                        dataset = agg.averageValidClickCount(dataset);
                        dataset = agg.clickTimeDelta(dataset);
                        dataset = agg.countClickInTenMinutes(dataset);

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

                        Dataset<Row> output = assembler.transform(dataset);

                        VectorIndexerModel featureIndexer = new VectorIndexer()
                                .setInputCol("features")
                                .setOutputCol("indexedFeatures")
                                .setMaxCategories(2)
                                .fit(output);

                        // Split the result into training and test sets (30% held out for testing).
//                        Dataset<Row>[] splits = output.randomSplit(new double[]{0.7, 0.3});
//                        Dataset<Row> trainingData = splits[0];
//                        Dataset<Row> testData = splits[1];



                        // Train a detact.DecisionTreeionTree model.
                        DecisionTreeRegressor dt = new DecisionTreeRegressor()
                                .setFeaturesCol("indexedFeatures")
                                .setLabelCol("is_attributed")
                                .setMaxDepth(10);

                        // Chain indexer and tree in a Pipeline.
                        Pipeline pipeline = new Pipeline()
                                .setStages(new PipelineStage[]{featureIndexer, dt});

                        // Train model. This also runs the indexer.
                        PipelineModel model = pipeline.fit(output);

                        // save model
                        try {
                            model.save("./decisionTree");
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }

                        PipelineModel p_model = PipelineModel.load("./decisionTree");

                        // Make predictions.
                        Dataset<Row> predictions = p_model.transform(assembler.transform(dataset));
                        predictions = predictions.drop("app")
                                .drop("device")
                                .drop("os")
                                .drop("channel")
                                .drop("utc_click_time")
                                .drop("utc_attributed_time")
                                .drop("avg_valid_click_count")
                                .drop("click_time_delta")
                                .drop("count_click_in_ten_mins")
                                .drop("features")
                                .drop("indexedFeatures");
                        predictions.printSchema();
                        List<String> stringDataset_feat = predictions.toJSON().collectAsList();
                        String[] header_f = {"ip","is_attributed","prediction"};
                        table3.setModel(table_maker.getTableModel(stringDataset_feat, header_f));
//
//
//
                        current_state="400";
                    }





                    // 3nd Column Final results


                }
            }
        });

        add(south_pane, BorderLayout.SOUTH);


    }
}

class CsvFile_chooser extends JPanel{
    private JFileChooser chooser = new JFileChooser();
    private JTextField path_field = new JTextField(30);
    private JButton browser = new JButton("...");
    public File selected_file;
    boolean is_selected = false;
    public CsvFile_chooser(){
        setLayout(new FlowLayout());
        chooser.addChoosableFileFilter(new FileFilter() {
            @Override
            public boolean accept(File f) {
                if (f.isDirectory()) {
                    return true;
                } else {
                    return f.getName().toLowerCase().endsWith(".csv");
                }
            }

            @Override
            public String getDescription() {
                return "CSV files (*.csv)";
            }
        });
        add(path_field);
        add(browser);
        browser.addActionListener(new ActionListener(){

            public void actionPerformed(ActionEvent e) {
                Object obj = e.getSource();
                if((JButton)obj == browser){
                    if(chooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION){
                        selected_file = chooser.getSelectedFile();
                        String path = selected_file.getAbsolutePath();
                        path_field.setText(path);
                        is_selected = true;
                    }
                }
            }
        });
    }
}

class TableCreator extends JPanel {
    public DefaultTableModel model;

    public DefaultTableModel getTableModel(List<String> data, String[] header) {
        Object tabledata[][]={};
        DefaultTableModel model = new DefaultTableModel(tabledata,header);
        JTable jtable = new JTable(model);
        JScrollPane jScollPane = new JScrollPane(jtable);
        add(jScollPane);
        try {
            for(int i =0; i<data.size();i++){
                BufferedReader reader = getFileReader(data.get(i));
                String line = reader.readLine();


                line = line.replace("\"", "");
                line = line.replace("_", "");
                line = line.replaceAll("\\{|\\}","");
                line = line.replaceAll("\\w+:", "");

                Object [] temp= line.split(",");

                model.addRow(temp);

                reader.close();
            }

        } catch (Exception e) {
            System.out.println(e);
        }

        return model;
    }

    private BufferedReader getFileReader(String data) {

        BufferedReader reader = new BufferedReader(new StringReader(data));

        //  In your real application the data would come from a file

        //Reader reader = new BufferedReader( new FileReader(...) );

        return reader;
    }
}

class Aggregation {

    public Dataset<Row> loadCSVDataSet(String path, SparkSession spark){
        // Read SCV to DataSet
        return spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(path);

    }

    public Dataset<Row> changeTimestempToLong(Dataset<Row> dataset){
        // cast timestamp to long
        Dataset<Row> newDF = dataset.withColumn("utc_click_time", dataset.col("click_time").cast("long"));
        newDF = newDF.withColumn("utc_attributed_time", dataset.col("attributed_time").cast("long"));
        newDF = newDF.drop("click_time").drop("attributed_time");
        return newDF;
    }

    public Dataset<Row> averageValidClickCount(Dataset<Row> dataset){
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

    public Dataset<Row> clickTimeDelta(Dataset<Row> dataset){
        WindowSpec w = Window.partitionBy ("ip")
                .orderBy("utc_click_time");

        Dataset<Row> newDF = dataset.withColumn("lag(utc_click_time)", lag("utc_click_time",1).over(w));
        newDF = newDF.withColumn("click_time_delta", when(col("lag(utc_click_time)").isNull(),
                lit(0)).otherwise(col("utc_click_time")).minus(when(col("lag(utc_click_time)").isNull(),
                lit(0)).otherwise(col("lag(utc_click_time)"))));
        newDF = newDF.drop("lag(utc_click_time)");
        return newDF;
    }

    public Dataset<Row> countClickInTenMinutes(Dataset<Row> dataset){
        WindowSpec w = Window.partitionBy("ip")
                .orderBy("utc_click_time")
                .rangeBetween(Window.currentRow(),Window.currentRow()+600);

        Dataset<Row> newDF = dataset.withColumn("count_click_in_ten_mins",
                (count("utc_click_time").over(w)).minus(1));    //TODO 본인것 포함할 것인지 정해야함.
        return newDF;
    }
}


