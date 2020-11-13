package com.trendcore.learning.apache.spark.connector.custom;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class TestCustomDataFrame {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                                        .builder()
                                        .master("spark://localhost:7077")
                                        .getOrCreate();

        Map map = new HashMap<>();

        DataFrameReader myCustomDataSource = sparkSession
                                                .read()
                                                .options(map)
                                                .format("myCustomDataSource");

        Dataset<Row> dataframe = myCustomDataSource.load();

        dataframe.show();

        dataframe.createOrReplaceTempView("actors");

        sparkSession
            .sql("select * from actors where id > 3")
            .show(false);

    }

}
