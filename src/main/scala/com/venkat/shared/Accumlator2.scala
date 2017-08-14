package com.venkat.shared

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Venkatram on 7/9/2017.
  */
object Accumlator2 extends App{

  val conf = new SparkConf() // Defining Spark Configuration Object
    .setMaster("local[*]") // Setting Spark Master as Local
    .setAppName("Test Spark") // Spark Application name, this is optional
    .set("spark.executor.memory", "2g") //Setting Spark Execution Memory size

  val sc = new SparkContext(conf)
  val file = sc.textFile("cdr.txt")
  var blankLines = sc.accumulator(0, "Call data record")

  val lines = file.flatMap(line => line.split("")).collect()
  lines.foreach(line =>{
    if(line == ""){
      blankLines += 1
    }
  })

 /* val callSigns = file.flatMap( line => {
    if (line == "") {
      blankLines += 1
    } else {
      line.split(" ")
    }
  })
  */
  println("Blank lines: " + blankLines.value)
}
