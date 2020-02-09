package com.trendcore.learning.apache.spark.rdd.groupByKey;

import groovy.lang.Tuple;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Create a Spark program to read the airport data from in/airports.text,
 * output the the list of the names of the airports located in each country.
 * <p>
 * Each row of the input file contains the following columns:
 * Airport ID, Name of airport, Main city served by airport,
 * Country where airport is located, IATA/FAA code,
 * ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
 * <p>
 * Sample output:
 * <p>
 * "Canada", List("Bagotville", "Montreal", "Coronation", ...)
 * "Norway" : List("Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..)
 * "Papua New Guinea",  List("Goroka", "Madang", ...)
 * ...
 */
public class AirportsByCountry {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AirportsByCountry").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = sparkContext.textFile("in/airports.text");

        JavaRDD<Tuple2<String, List<String>>> map = rdd.mapPartitionsWithIndex((v1, v2) -> {
            if (v1 == 0) {
                v2.next();
            }
            return v2;
        }, true).groupBy(v1 -> {
            String[] split = v1.split(",");
            return split[3];
        }).map(v1 -> {

            Iterator<String> iterator = v1._2.iterator();
            int i = 0;
            List<String> list = new ArrayList();
            while (iterator.hasNext()) {
                String next = iterator.next();
                list.add(next.split(",")[1]);
            }

            return new Tuple2<>(v1._1, list);
        });


        map.saveAsTextFile("output/airports_by_country_group_by");

        /*collect.forEach(stringIterableTuple2 -> {
            System.out.println(stringIterableTuple2._1 + " " + stringIterableTuple2._2);
        });*/

    }

}
