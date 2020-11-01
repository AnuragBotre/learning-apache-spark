package com.trendcore.learning.apache.spark.connector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
import java.util.Scanner;

public class JdbcDataFrameExample {

    public static void main(String[] args) throws AnalysisException {

        //Logger.getLogger("org").setLevel(Level.OFF);
        //Logger.getLogger("akka").setLevel(Level.OFF);
        //http://192.168.1.11:4040/
        //http://localhost:4040/

        SparkSession sparkSession = SparkSession
                                        .builder()
                                        .master("local[*]")
                                        .getOrCreate();

        String jdbcUrl = "jdbc:mysql://localhost:3306/sakila";

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "anurag");
        connectionProperties.put("password", "anurag");

        Dataset<Row> actors = sparkSession.read().jdbc(jdbcUrl, "actor", connectionProperties).as("actors");

        actors.createOrReplaceTempView("actors");

        actors.printSchema();
        actors.show();

        Dataset<Row> repartitionedActorsDataFrame = actors.repartition(2);

        repartitionedActorsDataFrame.show();

        Dataset<Row> sql = sparkSession.sql("SELECT * FROM actors a where a.actor_id > 30 ");

        sql.show(false);

        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }
}
