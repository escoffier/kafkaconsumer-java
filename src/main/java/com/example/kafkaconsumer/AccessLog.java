package com.example.kafkaconsumer;

public class AccessLog {

    private String ip;
    private int port;
    private String url;


    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "AccessLog{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", url='" + url + '\'' +
                '}';
    }
}
