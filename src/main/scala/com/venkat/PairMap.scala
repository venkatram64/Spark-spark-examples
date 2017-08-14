package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object PairMap extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  val lines = sc.textFile("cust_data.txt")
  val pairs = lines.map(x => (x.split(",")(0),1))
  val collect = pairs.collect()

  collect.foreach(println)

  //pairs.foreach(println)

}
