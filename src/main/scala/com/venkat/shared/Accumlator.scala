package com.venkat.shared

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object Accumlator extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
  val accum = sc.accumulator(0, "My Accumlator")

  val total = sc.parallelize(Array(1,2,3,4)).foreach(x => accum += x)

  println(accum.value)

}
