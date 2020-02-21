package com.trendcore.learning.apache.spark.rdd.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Function1;

import java.io.Serializable;

public class StackOverflowSurvey {

    private static String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AirportsInUSA").setMaster("local[*]");
        SparkContext s = new SparkContext(sparkConf);
        RDD<String> rdd = s.textFile("in/2016-stack-overflow-survey-responses.csv",2);

        LongAccumulator longAccumulator = s.longAccumulator();
        DoubleAccumulator missingSalaryMidPoint = s.doubleAccumulator();

        Filter filter = new Filter(longAccumulator,missingSalaryMidPoint);

        long count = rdd.filter(filter).count();

        System.out.println("Count of responses from Canada: " + count);
        System.out.println("Total Count of Responses: " + longAccumulator.count());
        System.out.println("Count of responses missing salary middle point: " + missingSalaryMidPoint.count());
    }

    public static class Filter implements Function1 , Serializable {

        private final LongAccumulator total;
        private final DoubleAccumulator missingSalaryMidPoint                ;

        public Filter(LongAccumulator longAccumulator, DoubleAccumulator missingSalaryMidPoint) {
            this.total = longAccumulator;
            this.missingSalaryMidPoint = missingSalaryMidPoint;
        }

        @Override
        public Object apply(Object v1) {
            String response = (String) v1;
            String splits[] = response.split(COMMA_DELIMITER, -1);
            total.add(1);

            if (splits[14].isEmpty()) {
                missingSalaryMidPoint.add(1);
            }

            return splits[2] == "Canada";
        }
    }

}
