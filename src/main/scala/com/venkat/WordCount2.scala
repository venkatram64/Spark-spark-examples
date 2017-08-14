package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object WordCount2 extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
  val input = sc.textFile("wordCount.txt")
  val words = input.flatMap(x => x.split(" "))
  val result = words.map(x => (x,1)).reduceByKey((x,y) => x + y).sortByKey()
  val collect = result.collect()
  collect.foreach(println)

}
