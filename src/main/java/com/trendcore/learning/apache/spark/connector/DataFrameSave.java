package com.trendcore.learning.apache.spark.connector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DataFrameSave {

    public static void main(String[] args) {

        //https://docs.databricks.com/data/data-sources/sql-databases.html

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
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

        sparkSession
                .table("actors")
                .write()
                .mode(SaveMode.ErrorIfExists)
                //.bucketBy(40,"first_name")
                .jdbc(jdbcUrl, "spark_actors", connectionProperties);
    }
}
