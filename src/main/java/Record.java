import scala.Serializable;

public class Record implements Serializable {
    Integer ip;
    Integer app;
    Integer device;
    Integer os;
    Integer channel;
    String clickTime;
    String attributedTime;
    Integer isAttributed;
    Integer clickInTenMins;

    // constructor , getters and setters
    public Record(int pIp, int pApp, int pDevice, int pOs, int pChannel, String pClickTime, String pAttributedTime, Integer pIsAttributed) {
        ip = new Integer(pIp);
        app = new Integer(pApp);
        device = new Integer(pDevice);
        os = new Integer(pOs);
        channel = new Integer(pChannel);
        clickTime = pClickTime;
        attributedTime = pAttributedTime;
        isAttributed = new Integer(pIsAttributed);
        clickInTenMins = new Integer(0);
    }

    public Record(int pIp, int pApp, int pDevice, int pOs, int pChannel, String pClickTime, String pAttributedTime, Integer pIsAttributed, int pClickInTenMins) {
        ip = new Integer(pIp);
        app = new Integer(pApp);
        device = new Integer(pDevice);
        os = new Integer(pOs);
        channel = new Integer(pChannel);
        clickTime = pClickTime;
        attributedTime = pAttributedTime;
        isAttributed = new Integer(pIsAttributed);
        clickInTenMins = new Integer(pClickInTenMins);
    }

    public Integer getIp() {
        return ip;
    }

    public void setIp(Integer ip) {
        this.ip = ip;
    }

    public Integer getApp() {
        return app;
    }

    public void setApp(Integer app) {
        this.app = app;
    }

    public Integer getDevice() {
        return device;
    }

    public void setDevice(Integer device) {
        this.device = device;
    }

    public Integer getOs() {
        return os;
    }

    public void setOs(Integer os) {
        this.os = os;
    }

    public Integer getChannel() {
        return channel;
    }

    public void setChannel(Integer channel) {
        this.channel = channel;
    }

    public String getClickTime() {
        return clickTime;
    }

    public void setClickTime(String clickTime) {
        this.clickTime = clickTime;
    }

    public String getAttributedTime() {
        return attributedTime;
    }

    public void setAttributedTime(String attributedTime) {
        this.attributedTime = attributedTime;
    }

    public Integer getAttributed() {
        return isAttributed;
    }

    public void setAttributed(Integer attributed) {
        isAttributed = attributed;
    }

    public Integer getClickInTenMins() {
        return clickInTenMins;
    }

    public void setClickInTenMins(Integer clickInTenMins) {
        this.clickInTenMins = clickInTenMins;
    }
}