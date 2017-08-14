package com.venkat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object ReduceByKey extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  val words = Array("a","b","a","a","b","b","a","a","b","b","b")
  val wordPairsRDD = sc.parallelize(words).map(word => (word,1))
  val wc = wordPairsRDD.reduceByKey((_ + _)).collect

  wc.foreach(println)

  val groupByKey = wordPairsRDD.groupByKey().collect

  groupByKey.foreach(println)

  //aggregateByKey

  val wordPairsRDD2 = sc.parallelize(Array("A","B","C","A","A")).map(word => (word,1))
  val a = wordPairsRDD2.aggregateByKey(0)((accum,v) => accum + v,(v1,v2) => v1 + v2).collect

  a.foreach(println)

  //sort by key

  val file = sc.textFile("error.txt")
  val filteredRows = file.filter(line => !line.contains("Error")).map(line => line.split(","))
  filteredRows.map(n => (n(0),n(2))).sortByKey().foreach(println _)

  filteredRows.map(n => (n(0),n(2))).sortByKey(ascending = false).foreach(println _)



}
