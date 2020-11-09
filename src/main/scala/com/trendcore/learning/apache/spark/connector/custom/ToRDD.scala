package com.trendcore.learning.apache.spark.connector.custom

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters
import scala.reflect.ClassTag

object ToRDD {

  def toRdd[T: ClassTag](sparkContext: SparkContext, list: java.util.List[T]): RDD[T] = {
    val seq = JavaConverters.asScalaIterator(list.iterator()).toSeq
    val rdd = sparkContext.parallelize(seq)
    rdd
  }

}
