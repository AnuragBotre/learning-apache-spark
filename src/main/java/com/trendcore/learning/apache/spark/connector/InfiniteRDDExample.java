package com.trendcore.learning.apache.spark.connector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.NextIterator;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;
import scala.reflect.ClassTag;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicLong;

public class InfiniteRDDExample {

    public static class Actor implements Serializable {

        private long actor_id;

        private String firstname;

        private String lastname;

        private long lastUpdate;

        public Actor(long counter, String firstname, String lastname, long currentTimeMillis) {
            this.actor_id = counter;
            this.firstname = firstname;
            this.lastname = lastname;
            this.lastUpdate = currentTimeMillis;
        }

        public long getActor_id() {
            return actor_id;
        }

        public void setActor_id(long actor_id) {
            this.actor_id = actor_id;
        }

        public String getFirstname() {
            return firstname;
        }

        public void setFirstname(String firstname) {
            this.firstname = firstname;
        }

        public String getLastname() {
            return lastname;
        }

        public void setLastname(String lastname) {
            this.lastname = lastname;
        }

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }
    }

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf()
                                .setAppName("Infinite RDD")
                                .setMaster("local[*]");

        SparkContext sparkContext = new SparkContext(sparkConf);

        InfiniteRDD infiniteRDD = new InfiniteRDD(sparkContext,null,ClassTag.apply(Actor.class));

        JavaRDD<Actor> actorJavaRDD = JavaRDD.fromRDD(infiniteRDD, ClassTag.apply(Actor.class));

        AtomicLong cnt = new AtomicLong();

        JavaRDD<Actor> repartitionedRdd = actorJavaRDD.repartition(200);

        /**
         * 'foreach' action will happen on executor node.
         */
        repartitionedRdd.foreach(actor -> {
            if(cnt.get() % 10000 == 0){
                System.out.println(
                        Thread.currentThread().getId()+"-"+Thread.currentThread().getName() +
                                " "+
                                actor.actor_id + " " +
                                actor.firstname + " " +
                                actor.lastname + " " +
                                actor.lastUpdate
                );
            }
            cnt.getAndIncrement();
        });

        /**
         * 'saveAsTextFile' action will happen on executor node.
         */
        //infiniteRDD.saveAsTextFile("");

        Scanner scanner = new Scanner(System.in);
        scanner.next();

    }

    public static class RddForEach implements Function1<Actor, BoxedUnit> , Serializable {

        long cnt = 0;

        @Override
        public BoxedUnit apply(Actor v1) {
            if(cnt % 10000 == 0){
                System.out.println(
                    Thread.currentThread().getId()+"-"+Thread.currentThread().getName() +
                        " "+
                        v1.actor_id + " " +
                        v1.firstname + " " +
                        v1.lastname + " " +
                        v1.lastUpdate
                );
            }
            cnt++;
            return null;
        }
    }

    public static class InfiniteRDD extends RDD<Actor> {

        private boolean next = true;

        public InfiniteRDD(SparkContext _sc, Seq deps, ClassTag evidence$1) {
            super(_sc, new ArraySeq<>(0), evidence$1);
        }

        @Override
        public Iterator<Actor> compute(Partition split, TaskContext context) {
            return new NextIterator() {

                private String names[] = {"ADAM","ANA","JAME","SADIO","THOMAS","ROBIE","DONALD","STEVE","TEJAS","NEO"};

                private String lastNames[] = {"ADRIAN","MADONA","TRUMP","RODRIGO","MANE","PARKER","PARTELY","SMITH","ANDERSON","SANE"};

                long noOfTimes = 0;

                long counter = 0;

                @Override
                public void close() {

                }

                @Override
                public boolean hasNext() {
                    return next;
                }

                @Override
                public Actor next() {

                    if(noOfTimes <= 2) {
                        if(counter >= 1000000){
                            noOfTimes++;
                            counter = 0;
                        }

                        Random random = new Random();
                        int i = random.nextInt(100) % 10;
                        counter++;
                        return new Actor(counter,names[i],lastNames[i],System.currentTimeMillis());
                    } else {
                        next = false;
                    }
                    return null;
                }

                @Override
                public Object getNext() {
                    return null;
                }
            };
        }

        @Override
        public Partition[] getPartitions() {
            Partition[] partitions = new Partition[1];
            partitions[0] = new Partition() {
                @Override
                public int index() {
                    return 0;
                }
            };
            return partitions;
        }
    }
}
