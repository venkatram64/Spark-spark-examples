package com.venkat.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Venkatram on 7/10/2017.
  */
object SparkSQL extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)

  //val sqlContext = new SQLContext(sc)
  val hiveContext = new HiveContext(sc)
  val file = hiveContext.jsonFile("employee.json")
  file.registerTempTable("emp")
  hiveContext.cacheTable("emp")
  println(hiveContext.isCached("emp"))
  val empName = hiveContext.sql("select name from emp").collect

  empName.foreach(e => println(e))

}
