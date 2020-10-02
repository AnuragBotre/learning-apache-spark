package com.trendcore.learning.apache.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PairRddWithMoreElementsPerKey {

    static class Person implements Serializable {

        private String username;

        private String lastname;

        public Person(String username, String lastname) {
            this.username = username;
            this.lastname = lastname;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "username='" + username + '\'' +
                    ", lastname='" + lastname + '\'' +
                    '}';
        }
    }

    static class PersonInfo implements Serializable {

        private String username;

        private int age;

        public PersonInfo(String username, int age) {
            this.username = username;
            this.age = age;
        }

        @Override
        public String toString() {
            return "PersonInfo{" +
                    "username='" + username + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("PairRddWithMoreElements")
                .setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Person> list = new ArrayList<>();
        list.add(new Person("Adam","B"));
        list.add(new Person("Adam","Doe"));
        list.add(new Person("John","Walter"));
        list.add(new Person("John","Potter"));

        JavaRDD<Person> rdd = sparkContext.parallelize(list);

        /**
         * One key map to different objects.
         */
        JavaPairRDD<String,Person> personPairRdd = rdd.mapToPair(v1 -> new Tuple2(v1.username, v1));

        personPairRdd.foreach(s ->
                System.out.println(s._1 + " " + s._2));

        List<PersonInfo> personInfoList = new ArrayList<>();
        personInfoList.add(new PersonInfo("Adam",20));
        personInfoList.add(new PersonInfo("Adam",21));
        personInfoList.add(new PersonInfo("John",22));
        personInfoList.add(new PersonInfo("John",23));

        JavaRDD<PersonInfo> personInfoJavaRDD = sparkContext.parallelize(personInfoList);
        JavaPairRDD<String, PersonInfo> personInfoPairRDD = personInfoJavaRDD.mapToPair(personInfo -> new Tuple2<>(personInfo.username, personInfo));

        JavaPairRDD<String, Tuple2<Person, PersonInfo>> join = personPairRdd.join(personInfoPairRDD);

        join.foreach(s -> System.out.println(s._1 + " " + s._2));
    }

}
