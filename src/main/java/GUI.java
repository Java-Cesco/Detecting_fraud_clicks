import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.swing.*;
import java.awt.*;
import java.io.BufferedReader;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Vector;
import java.awt.BorderLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableModel;

public class GUI extends JFrame {
    JTabbedPane tab = new JTabbedPane();

    public GUI(List<String> q, String[][] data) {
        super("CESCO");

        tab.addTab("png", new PngPane());
        tab.addTab("gif", new GifPane());
        tab.addTab("jpg", new JpgPane());
        tab.addTab("table", new createTable(q));
        tab.addTab("processed_features", new createTable_alter(data));

        add(tab);

        setSize(800, 500); // 윈도우의 크기 가로x세로
        setVisible(true); // 창을 보여줄떄 true, 숨길때 false
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE); // x 버튼을 눌렀을때 종료
    }

//    public static void main(String args[]) {
//        new GUI();
//    }
}

class PngPane extends JPanel {
    public PngPane() {
        super();
        ImageIcon image = new ImageIcon("data/model.png");
        JLabel label = new JLabel("", image, JLabel.CENTER);
        setLayout(new BorderLayout());
        add(label, BorderLayout.CENTER);
    }
}

class GifPane extends JPanel {
    public GifPane() {
        super();
        ImageIcon image = new ImageIcon("data/model.gif");
        JLabel label = new JLabel("", image, JLabel.CENTER);
        setLayout(new BorderLayout());
        add(label, BorderLayout.CENTER);
    }
}

class JpgPane extends JPanel {
    public JpgPane() {
        super();
        ImageIcon image = new ImageIcon("data/model.jpg");
        JLabel label = new JLabel("", image, JLabel.CENTER);
        setLayout(new BorderLayout());
        add(label, BorderLayout.CENTER);
    }
}

class createTable_alter extends JPanel{
    private String[] header = {"ip","app","device","os","channel","is_attributed","click_time","attributed_time",
            "avg_valid_click_count","click_time_delta","count_click_in_tenmin"};
/*
root
 |-- ip: integer (nullable = true)
 |-- app: integer (nullable = true)
 |-- device: integer (nullable = true)
 |-- os: integer (nullable = true)
 |-- channel: integer (nullable = true)
 |-- is_attributed: integer (nullable = true)
 |-- utc_click_time: long (nullable = true)
 |-- utc_attributed_time: long (nullable = true)
 |-- avg_valid_click_count: double (nullable = true)
 |-- click_time_delta: long (nullable = true)
 |-- count_click_in_ten_mins: long (nullable = false)
 */
    public createTable_alter(String[][] data){
        JTable processed_table = new JTable(data, header);
        JScrollPane jScrollPane = new JScrollPane(processed_table);
        add(jScrollPane);
    }
}

class createTable extends JPanel {
    long start = System.currentTimeMillis();
    public createTable(List<String> data) { //constructor : display table
        getTableModel(data);
    }

    private DefaultTableModel getTableModel(List<String> data) {
        String column_n[]={"ip","app","device","os","channel","is_attributed","click_time",
                "avg_valid_click_count","click_time_delta","count_click_in_tenmin"};
        Object tabledata[][]={};
        DefaultTableModel model = new DefaultTableModel(tabledata,column_n);
        JTable jtable = new JTable(model);
        JScrollPane jScollPane = new JScrollPane(jtable);
        add(jScollPane);
        try {
            for(int i =0; i<data.size();i++){
                BufferedReader reader = getFileReader(data.get(i));
                String line = reader.readLine();


                line = line.replace("\"", "");
                line = line.replace("_", "");
                //line = line.replace("\\{","");
                line = line.replaceAll("\\{|\\}","");
                line = line.replaceAll("\\w+:", "");

                //System.out.println(line);
                Object [] temp= line.split(",");

                model.addRow(temp);

                reader.close();
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("Steve's Procedure2 time elapsed : " + (end-start)/1000.0);

        return model;
    }

    private BufferedReader getFileReader(String data) {

        BufferedReader reader = new BufferedReader(new StringReader(data));

        //  In your real application the data would come from a file

        //Reader reader = new BufferedReader( new FileReader(...) );

        return reader;
    }
}