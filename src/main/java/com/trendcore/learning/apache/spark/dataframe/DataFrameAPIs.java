package com.trendcore.learning.apache.spark.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DataFrameAPIs {

    public static void main(String[] args) {
        /**
         * If we want to override command line argument
         * in the spark job then only use below constructor.
         * SparkConf sparkConf = new SparkConf()
         * */
        SparkConf sparkConf = new SparkConf().setAppName("DataFrameExample").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        /**
         * sql context is required to create data frames in  spark
         * */
        SQLContext sqlContext = new SQLContext(sparkContext);


        /*This will create schema.
        This is schema on read as we are creating schema while reading. */
        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("empid",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("salary",DataTypes.IntegerType,true));

        StructType schema = DataTypes.createStructType(fieldList);

        /**
         * Read data from the text file.
         */
        JavaRDD<String> rdd = sparkContext.textFile("in/emp.csv");
        JavaRDD<Row> rowRDD = rdd.mapPartitionsWithIndex((v1, v2) -> {
            if(v1 == 0){
                v2.next();
            }
            return v2;
        },true).map(v1 -> {
            String[] split = v1.split(",");
            return RowFactory.create(split[0], split[1], Integer.valueOf(split[2]));
        });

        /**
         * create data frame.
         */
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRDD, schema);
        /**
         * Register table so that it can be used in sql queries.
         */
        dataFrame.registerTempTable("employee");

        /**
         * This will print schema.
         */
        dataFrame.printSchema();

        Dataset<Row> filter = dataFrame.filter(new Filter());

        /**
         * These are actions on dataframe api.
         * They will trigger RDD execution.
         */
        filter.show();
        /**
         * They will trigger RDD execution.
         * */
        filter.collect();
        Row[] take = (Row[]) filter.take(0);
        for (Row row : take) {
            System.out.println(row);
        }

        Scanner scanner = new Scanner(System.in);
        scanner.next();
    }

    static class Filter implements Function1<Row, Object> , Serializable {

        @Override
        public Boolean apply(Row v1) {
            return v1.get(0).equals("John");
        }

    }
}
