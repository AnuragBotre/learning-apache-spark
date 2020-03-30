package com.trendcore.learning.apache.spark.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DataFrameExample {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("DataFrameExample").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sparkContext);

        String schemaString = "name,empid,salary";

        List<StructField> fieldList = new ArrayList<>();
        fieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("empid",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("salary",DataTypes.IntegerType,true));

        StructType schema = DataTypes.createStructType(fieldList);

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

        Dataset<Row> dataFrame = sqlContext.createDataFrame(rowRDD, schema);
        dataFrame.registerTempTable("employee");

        dataFrame.printSchema();

        Dataset<Row> sql = sqlContext.sql("select * from employee order by salary desc limit 1");

        sql.toJavaRDD().foreach(row -> System.out.println(row.get(0) + " " + row.get(1) + " " + row.get(2)));
    }

}
