package com.venkat.shared

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/10/2017.
  */
object Broadcast extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  val names = sc.parallelize(List(("www.google.com","Google"),("www.yahoo.com","yahoo")))
  val visits = sc.parallelize(List(("www.google.com",90),("www.yahoo.com",10)))

  val pageMap = names.collect.toMap //converted to map rdd
  val bcMap = sc.broadcast(pageMap)

  val joined = visits.map{
    case (url, counts) =>
      (url, (pageMap(url),counts))
  }

  val collected = joined.collect()

  collected.foreach(println)

}
