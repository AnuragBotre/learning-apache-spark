package com.trendcore.learning.apache.spark.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.max;

public class SparkHandsOnExample1 {

    public static void main(String[] args) {

        Scanner scanner = new Scanner(System.in);

        //Spark UI
        //http://localhost:4040/

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("KDD Cup 1999 Analysis");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = javaSparkContext
                .textFile("in/kddcup.data_10_percent.gz");

        JavaRDD<String[]> map = rdd.map(v1 -> v1.split(","));

        map.take(10).stream().forEach(strings -> {
            System.out.println(Arrays.stream(strings).collect(Collectors.joining(",")));
        });


        JavaRDD<ParseRdd> parseRdd = map.map(v1 -> new ParseRdd(v1));

        List<ParseRdd> parseRddDataView = parseRdd.take(5);
        show(scanner,parseRddDataView);

        SparkSession sparkSession = new SparkSession(javaSparkContext.sc());

        Dataset<Row> dataFrame = sparkSession.createDataFrame(parseRdd, ParseRdd.class);

        dataFrame.createOrReplaceTempView("connections");

        dataFrame.printSchema();

        dataFrame.show(false);
        scanner.next();

        StringToColumnDelegator s = new StringToColumnDelegator(sparkSession);

        Dataset<Row> rowDataset = dataFrame
                .groupBy("protocol_type")
                .count()
                .orderBy(s.to("count").desc());

        rowDataset.show(false);
        scanner.next();

        Dataset<Row> noOfProtocolType = sparkSession
                .sql("SELECT count(*) as count , protocol_type " +
                        "FROM connections " +
                        " group by protocol_type " +
                        " order by count desc");

        noOfProtocolType.show(false);
        scanner.next();


        //Connections based on protocols and attacks
        /*
        Which protocols are most vulnerable to attacks by using the following SQL query.
        * */
        Dataset<Row> mostVulnerableProtocol = sparkSession.sql("SELECT protocol_type , " +
                "CASE label " +
                "   WHEN 'normal.' THEN 'No Attack' " +
                "   ELSE 'attack'" +
                "END as state," +
                "count(*) as Freq" +
                " FROM connections " +
                " group by state , protocol_type " +
                " order by 3 desc");

        mostVulnerableProtocol.show(false);
        scanner.next();


        /*
        Connection stats based on protocols and attacks.
         statistical measures pertaining to these protocols and attacks for our connection requests.
         */

        Dataset<Row> connectionStatsBasedOnProtocolAndAttacks = sparkSession.sql("SELECT protocol_type," +
                " CASE label" +
                "   WHEN 'normal.' THEN 'no attack'" +
                "   ELSE 'attack'" +
                "END as state," +
                "count(*) as total_freq," +
                "ROUND(AVG(duration), 2) as mean_duration," +
                "SUM(numFailedLogins) as total_failed_logins, " +
                "SUM(numCompromised) as total_compromised," +
                "SUM(numFileCreations) as total_file_creations," +
                "SUM(suAttempted) as total_root_attempts," +
                "SUM(numRoot) as total_root_acceses" +
                " FROM connections" +
                " GROUP BY protocol_type , state" +
                " order by 3 desc");

        connectionStatsBasedOnProtocolAndAttacks.show(false);
        scanner.next();

        /*
        Filtering connection stats based on the TCP protocol by service and attack type
         */

        Dataset<Row> tcpAttackStats = sparkSession.sql("SELECT protocol_type," +
                " CASE label" +
                "   WHEN 'normal.' THEN 'no attack'" +
                "   ELSE 'attack'" +
                "END as state," +
                "count(*) as total_freq," +
                "ROUND(AVG(srcBytes), 2) as mean_src_bytes," +
                "ROUND(AVG(destBytes), 2) as mean_dst_bytes," +
                "ROUND(AVG(duration), 2) as mean_duration," +
                "SUM(numFailedLogins) as total_failed_logins, " +
                "SUM(numCompromised) as total_compromised," +
                "SUM(numFileCreations) as total_file_creations," +
                "SUM(suAttempted) as total_root_attempts," +
                "SUM(numRoot) as total_root_acceses" +
                " FROM connections" +
                " where protocol_type = 'tcp' AND label != 'normal.' " +
                " GROUP BY protocol_type , state" +
                " order by 3 desc");

        tcpAttackStats.show(false);
        scanner.next();

        /*
        Filtering connection stats based on the TCP protocol by service and attack type
         */
        tcpAttackStats = sparkSession.sql("SELECT service," +
                "label as attack_type," +
                "count(*) as total_freq," +
                "ROUND(AVG(srcBytes), 2) as mean_src_bytes," +
                "ROUND(AVG(destBytes), 2) as mean_dst_bytes," +
                "ROUND(AVG(duration), 2) as mean_duration," +
                "SUM(numFailedLogins) as total_failed_logins, " +
                "SUM(numCompromised) as total_compromised," +
                "SUM(numFileCreations) as total_file_creations," +
                "SUM(suAttempted) as total_root_attempts," +
                "SUM(numRoot) as total_root_acceses" +
                " FROM connections" +
                " where protocol_type = 'tcp' AND label != 'normal.' " +
                " GROUP BY service , label " +
                " Having (mean_duration >= 50 AND total_file_creations >= 5" +
                        " AND total_root_acceses >= 1) "+
                " order by total_freq desc");

        tcpAttackStats.show(false);
        scanner.next();

        /*
        Subqueries to filter TCP attack types based on service
         */

        tcpAttackStats = sparkSession.sql("SELECT t.service , " +
                " t.attack_type , t.total_freq " +
                " FROM ( SELECT service," +
                "label as attack_type," +
                "count(*) as total_freq," +
                "ROUND(AVG(srcBytes), 2) as mean_src_bytes," +
                "ROUND(AVG(destBytes), 2) as mean_dst_bytes," +
                "ROUND(AVG(duration), 2) as mean_duration," +
                "SUM(numFailedLogins) as total_failed_logins, " +
                "SUM(numCompromised) as total_compromised," +
                "SUM(numFileCreations) as total_file_creations," +
                "SUM(suAttempted) as total_root_attempts," +
                "SUM(numRoot) as total_root_acceses" +
                " FROM connections" +
                " where protocol_type = 'tcp' AND label != 'normal.' " +
                " GROUP BY service , attack_type" +
                " Order by total_freq desc ) as t ");

        tcpAttackStats.show(false);
        scanner.next();

        /*
        Build a pivot table from aggregated data
         */

        Dataset<Row> pivotTable = tcpAttackStats
                .groupBy(s.to("service"))
                .pivot("attack_type")
                .agg(max(s.to("total_freq")))
                .na()
                .fill(0);

        pivotTable.show(false);
        scanner.next();

    }

    public static class ParseRdd implements Serializable {
        private int duration;

        private String protocol_type;

        private String service;

        private String flag;

        private int srcBytes;

        private int destBytes;

        private int wrongFragment;

        private int urgent;

        private int hot;

        private int numFailedLogins;

        private int numCompromised;

        private String suAttempted;

        private int numRoot;

        private int numFileCreations;

        private String label;

        public ParseRdd(String[] r) {
            duration = Integer.parseInt(r[0]);
            protocol_type = r[1];
            service = r[2];
            flag = r[3];
            srcBytes = Integer.parseInt(r[4]);
            destBytes = Integer.parseInt(r[5]);
            wrongFragment = Integer.parseInt(r[7]);
            urgent = Integer.parseInt(r[8]);
            hot = Integer.parseInt(r[9]);
            numFailedLogins = Integer.parseInt(r[10]);
            numCompromised = Integer.parseInt(r[12]);
            suAttempted = r[14];
            numRoot = Integer.parseInt(r[15]);
            numFileCreations = Integer.parseInt(r[16]);
            label = r[r.length - 1];
        }

        public int getDuration() {
            return duration;
        }

        public void setDuration(int duration) {
            this.duration = duration;
        }

        public String getProtocol_type() {
            return protocol_type;
        }

        public void setProtocol_type(String protocol_type) {
            this.protocol_type = protocol_type;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public String getFlag() {
            return flag;
        }

        public void setFlag(String flag) {
            this.flag = flag;
        }

        public int getSrcBytes() {
            return srcBytes;
        }

        public void setSrcBytes(int srcBytes) {
            this.srcBytes = srcBytes;
        }

        public int getDestBytes() {
            return destBytes;
        }

        public void setDestBytes(int destBytes) {
            this.destBytes = destBytes;
        }

        public int getWrongFragment() {
            return wrongFragment;
        }

        public void setWrongFragment(int wrongFragment) {
            this.wrongFragment = wrongFragment;
        }

        public int getUrgent() {
            return urgent;
        }

        public void setUrgent(int urgent) {
            this.urgent = urgent;
        }

        public int getHot() {
            return hot;
        }

        public void setHot(int hot) {
            this.hot = hot;
        }

        public int getNumFailedLogins() {
            return numFailedLogins;
        }

        public void setNumFailedLogins(int numFailedLogins) {
            this.numFailedLogins = numFailedLogins;
        }

        public int getNumCompromised() {
            return numCompromised;
        }

        public void setNumCompromised(int numCompromised) {
            this.numCompromised = numCompromised;
        }

        public String getSuAttempted() {
            return suAttempted;
        }

        public void setSuAttempted(String suAttempted) {
            this.suAttempted = suAttempted;
        }

        public int getNumRoot() {
            return numRoot;
        }

        public void setNumRoot(int numRoot) {
            this.numRoot = numRoot;
        }

        public int getNumFileCreations() {
            return numFileCreations;
        }

        public void setNumFileCreations(int numFileCreations) {
            this.numFileCreations = numFileCreations;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }
    }

    private static void show(Scanner scanner,List take) {
        scanner.next();
        take.stream().forEach(s -> System.out.println(s));
    }

}
