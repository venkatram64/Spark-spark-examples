package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object ParallelEx2 extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  val parallel = sc.parallelize(1 to 9)
  val collect = parallel.mapPartitionsWithIndex((index:Int, it:Iterator[Int]) => it.toList.map(x=>index + ", " + x).iterator).collect

  collect.foreach(println)
  //setting name
  parallel.setName("MyTestRDD");
  println(parallel.name)

  println("Max value " + parallel.max)
  println("Min value " + parallel.min)

  parallel.saveAsObjectFile("myTest")
}
