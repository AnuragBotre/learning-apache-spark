package com.trendcore.learning.apache.spark.dataframe;

import com.sun.tools.javac.code.TypeTag;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.ScalaReflection;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.expressions.SparkUserDefinedFunction;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import scala.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;
import scala.reflect.api.TypeTags;

import java.io.Serializable;
import java.lang.Long;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.udf;

public class UserDefinedFunctionJava {

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

        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

        String s = "\"{\"device_id\": 1, \"device_type\": \"sensor-igauge\", \"ip\": \"213.161.254.1\", \"cca3\": \"NOR\", \"cn\": \"Norway\", \"temp\": 30, \"signal\": 18, \"battery_level\": 6, \"c02_level\": 1413, \"timestamp\" :1475600498 }\"";

        System.out.println(s);

        //StructType schema = new StructType().add(new StructField("deviceData",DataTypes.StringType,false,Metadata.empty()));

        List<Device> ts = Arrays.asList(
            new Device("1","sensor-igauge","213.161.254.1","US","cn",30L,6),
            new Device("2","sensor-ipad","213.161.254.1","IN","cn",30L,6)
        );
        Dataset<Row> deviceDataTable = sparkSession.createDataFrame(ts, Device.class).as("DeviceData");

        deviceDataTable.printSchema();

        deviceDataTable.show();

        GenericRow row = new GenericRow(new Object[]{"1","sensor-igauge","213.161.254.1","US","cn",30L,6});

        List<Row> listOfRows = new ArrayList<>();
        listOfRows.add(row);

        StructType schema = new StructType()
                            .add("dcId", DataTypes.StringType,false, Metadata.empty())
                            .add("deviceType", DataTypes.StringType,false, Metadata.empty())
                            .add("ip", DataTypes.StringType,false, Metadata.empty())
                            .add("cca3", DataTypes.StringType,false, Metadata.empty())
                            .add("cn", DataTypes.StringType,false, Metadata.empty())
                            .add("temp", DataTypes.LongType,false, Metadata.empty())
                            .add("batteryLevel", DataTypes.IntegerType,false, Metadata.empty());

        Dataset<Row> dataFrameFromSchema = sparkSession
                                    .createDataFrame(listOfRows, schema)
                                    .as("dataFrameFromSchema");

        dataFrameFromSchema.printSchema();

        dataFrameFromSchema.show(false);

        UserDefinedFunction hashCodeUDF = udf(o -> o.hashCode(), DataTypes.IntegerType);
        sparkSession.udf().register("hashCodeGenerator",hashCodeUDF);



        Seq<Option<ScalaReflection.Schema>> inputTypesForFunction = JavaConverters.asScalaIterator(
                Arrays.asList(Option.apply(new ScalaReflection.Schema(DataTypes.LongType, false)),
                        Option.apply(new ScalaReflection.Schema(DataTypes.StringType, false))).iterator()
        ).toSeq();

        UserDefinedFunction myUserDefinedFunction = SparkUserDefinedFunction.create(new Function2<Long,String,String>() {
            @Override
            public String apply(Long v1, String v2) {
                return v1 + " " + v2;
            }
        }, DataTypes.StringType, inputTypesForFunction);

        sparkSession.udf().register("myUserDefinedFunction",myUserDefinedFunction);

        /*This line is required.
        Even if dataframe is created, it needs to be registered to use it in sql.*/
        dataFrameFromSchema.createOrReplaceTempView("dataFrameFromSchema");

        sparkSession
                .sql("SELECT dcId, hashCodeGenerator(dcId) as hashCode, " +
                        "myUserDefinedFunction(temp,deviceType)" +
                        "from dataFrameFromSchema")
                .show(false);
    }
}
