package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object ParallelEx extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
  val parallelize = sc.parallelize(1 to 9, 3)

  val rows = parallelize.mapPartitions(x => List(x.next).iterator).collect
  rows.foreach(r => println(r))

}
