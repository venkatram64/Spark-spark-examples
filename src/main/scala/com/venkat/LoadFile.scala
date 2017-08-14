package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object LoadFile extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  val data = sc.textFile("data.txt")
 /* val rows = data.map(line => line.split(","))

  val collect = rows.collect()

  collect.foreach(ar => {
    ar.foreach(e => println(e))
  })*/

  val rows = data.flatMap(line => line.split(","))
  val collect = rows.collect()
  collect.foreach(e => println(e))

}

