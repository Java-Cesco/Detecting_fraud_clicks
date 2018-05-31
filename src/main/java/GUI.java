import javax.swing.*;
import java.awt.*;

public class GUI extends JFrame {
    JTabbedPane tab = new JTabbedPane();
    public GUI() {
        super("CESCO");

        tab.addTab("png", new PngPane());
        tab.addTab("gif",new GifPane());
        tab.addTab("jpg",new JpgPane());

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
        add( label, BorderLayout.CENTER );
    }
}

class JpgPane extends JPanel {
    public JpgPane() {
        super();
        ImageIcon image = new ImageIcon("data/model.jpg");
        JLabel label = new JLabel("", image, JLabel.CENTER);
        setLayout(new BorderLayout());
        add( label, BorderLayout.CENTER );
    }
}