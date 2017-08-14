package com.venkat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by Venkatram on 7/8/2017.
  */
object WordCount extends App {

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
  val lines = sc.parallelize(Seq("This is first line", "This is second line", "This is third line"))

  val counts = lines.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey((_ + _))

  counts.foreach(println)

}
