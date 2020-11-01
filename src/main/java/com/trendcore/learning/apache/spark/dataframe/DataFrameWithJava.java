package com.trendcore.learning.apache.spark.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class DataFrameWithJava {

    public static class Device implements Serializable {
        private String dcId;

        private String deviceType;

        private String ip;

        private String cca3;

        private String cn;

        private long temp;

        private int batteryLevel;

        public Device(String dcID,
                      String deviceType,
                      String ip,
                      String cca3,
                      String cn,
                      long temp,
                      int batteryLevel) {
            this.dcId = dcID;
            this.deviceType = deviceType;
            this.ip = ip;
            this.cca3 = cca3;
            this.cn = cn;
            this.temp = temp;
            this.batteryLevel = batteryLevel;
        }

        public String getDcId() {
            return dcId;
        }

        public void setDcId(String dcId) {
            this.dcId = dcId;
        }

        public String getDeviceType() {
            return deviceType;
        }

        public void setDeviceType(String deviceType) {
            this.deviceType = deviceType;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getCca3() {
            return cca3;
        }

        public void setCca3(String cca3) {
            this.cca3 = cca3;
        }

        public String getCn() {
            return cn;
        }

        public void setCn(String cn) {
            this.cn = cn;
        }

        public long getTemp() {
            return temp;
        }

        public void setTemp(long temp) {
            this.temp = temp;
        }

        public int getBatteryLevel() {
            return batteryLevel;
        }

        public void setBatteryLevel(int batteryLevel) {
            this.batteryLevel = batteryLevel;
        }
    }

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkSession sparkSession = SparkSession
                                    .builder()
                                    .master("local[*]")
                                    .getOrCreate();

        sparkSession.read().json("");

        List<Device> ts = Arrays.asList(
                new Device("1","sensor-igauge","213.161.254.1","US","cn",30L,6),
                new Device("2","sensor-ipad","213.161.254.1","IN","cn",30L,6)
        );
        Dataset<Row> deviceDataTable = sparkSession.createDataFrame(ts, Device.class);

        deviceDataTable.printSchema();

        deviceDataTable.show();

        StringToColumnDelegator s = new StringToColumnDelegator(sparkSession);

        Column dcIdIsGreaterThanBatteryLevel = s.to("dcId").gt(s.to("batteryLevel"));

        deviceDataTable
                .select(dcIdIsGreaterThanBatteryLevel)
                .show();

        deviceDataTable
                .select(s.to("dcId"),
                        s.to("batteryLevel"),
                        dcIdIsGreaterThanBatteryLevel
                        )
                .filter("dcId != batteryLevel")
                .show();

        deviceDataTable
                .select(s.to("dcId"),
                        s.to("batteryLevel"),
                        dcIdIsGreaterThanBatteryLevel
                )
                .filter("dcId == batteryLevel")
                .show();

        
    }


}
