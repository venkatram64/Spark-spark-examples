package com.venkat.shared

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/10/2017.
  */
object MiscEx extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
//numeric rdd functions
  val data = sc.parallelize(Array(1,2,3,4,5,6,7,8,9))

  println("Count: " +data.count)
  println("Min: " + data.min)
  println("Max: " + data.max)
  println("Mean: " + data.mean())
  println("Sum: " + data.sum())
  println("Variance: " + data.variance())
  println("Std Deviation: " + data.stdev())

}
