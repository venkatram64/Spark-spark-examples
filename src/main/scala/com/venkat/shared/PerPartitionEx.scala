package com.venkat.shared

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/10/2017.
  */
object PerPartitionEx extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
  val data = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),3)
  data.foreachPartition(x => println(x.reduce(_+_)))

  val dataPar = sc.parallelize(Array("a","b","c","d","e","f","g","h","i","j"),2)

  def myFun(index:Int, iter:Iterator[String]): Iterator[String] = {
    iter.toList.map(x => "[PartId: " + index + ", val: " + x + "]").iterator
  }

  val reData = dataPar.mapPartitionsWithIndex(myFun).collect()
  reData.foreach(println)

}
