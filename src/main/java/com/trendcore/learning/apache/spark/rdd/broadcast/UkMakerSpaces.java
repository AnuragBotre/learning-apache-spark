package com.trendcore.learning.apache.spark.rdd.broadcast;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class UkMakerSpaces {

    static String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("AirportsInUSA").setMaster("local[*]");
        ;
        JavaSparkContext s = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd = s.textFile("in/uk-makerspaces-identifiable-data.csv");

        Broadcast<Map<String, String>> broadcast = s.broadcast(getPostalCodeMap());

        JavaRDD<String> map = rdd.map(v1 -> v1.split(COMMA_DELIMITER))
                .filter(v1 -> !"Timestamp".equals(v1[0]))
                .filter(v1 -> getPostPrefix(v1).isPresent())
                .map(v1 -> {
                    return broadcast.value().get(getPostPrefix(v1).orElse(null));
                });

        map.countByValue().forEach((s1, aLong) -> System.out.println(s1 + " " + aLong));

    }

    private static Optional<Object> getPostPrefix(String[] v1) {
        String postcode = v1[4];
        if (postcode.isEmpty())
            return Optional.empty();
        else
            return Optional.ofNullable(postcode.split(" ")[0]);
    }

    private static Map<String, String> getPostalCodeMap() throws IOException {

        Map<String, String> collect = Files.lines(new File("in/uk-postcode.csv").toPath())
                .map(line -> {
                    String[] split = line.split(COMMA_DELIMITER, -1);
                    return split;
                }).collect(Collectors.toMap(o -> o[0], o -> o[7]));
        return collect;
    }
}
